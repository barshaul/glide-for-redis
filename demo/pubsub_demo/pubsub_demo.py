import argparse
import asyncio
import os
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from glide import (
    ByAddressRoute,
    GlideClientConfiguration,
    GlideClusterClient,
    GlideClusterClientConfiguration,
    Logger,
    LogLevel,
    NodeAddress,
    PubSubMsg,
    ReadFrom,
    SlotKeyRoute,
    SlotType,
)
from quart import Quart, jsonify, render_template, request

Logger.set_logger_config(LogLevel.INFO)
PubSubChannelModes = GlideClusterClientConfiguration.PubSubChannelModes
PubSubSubscriptions = GlideClusterClientConfiguration.PubSubSubscriptions
app = Quart(__name__)

publisher_messages = deque()
listener_messages = deque()
cluster_info = {}

publisher = None
listener = None
channels = set(["foo", "foo2"])
# Global error counters
publisher_error_count = 0
listener_error_count = 0
last_publisher_error = ""
last_listener_error = ""


async def create_pubsub_clients():
    global channel
    cluster_channels_and_patterns = {PubSubChannelModes.Sharded: channels}
    pubsub_subscriptions = PubSubSubscriptions(
        channels_and_patterns=cluster_channels_and_patterns, callback=None, context=None
    )
    config = GlideClusterClientConfiguration(
        [NodeAddress(host="localhost", port=app.config["VALKEY_PORT"])],
        use_tls=False,
        pubsub_subscriptions=pubsub_subscriptions,
    )
    global publisher
    global listener
    publisher = await GlideClusterClient.create(config)
    listener = await GlideClusterClient.create(config)
    return publisher, listener


async def publish_messages():
    global channels, publisher_error_count, last_publisher_error
    while True:
        tasks = []
        successful_messages = []

        for idx, channel in enumerate(channels):
            timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
            message = f"channel_{idx+1}_{timestamp}"

            # Define the async task with error handling
            async def publish_and_record(message, channel):
                try:
                    await publisher.publish(message, channel, sharded=True)
                    # If successful, store the message
                    successful_messages.append(message)
                except Exception as e:
                    print(f"Error publishing to channel {channel}: {e}")
                    global publisher_error_count, last_publisher_error
                    publisher_error_count += 1
                    last_publisher_error = str(e)

            # Append the task to the list
            tasks.append(publish_and_record(message, channel))

        # Run all publish tasks concurrently
        await asyncio.gather(*tasks)

        # Sort successful messages (default sort is sufficient due to message structure)
        successful_messages.sort()

        # Insert sorted messages into publisher_messages
        for message in successful_messages:
            publisher_messages.appendleft(message)

        await asyncio.sleep(1)


async def listen_messages():
    global listener, listener_error_count, last_listener_error
    while True:
        try:
            msg = await listener.get_pubsub_message()
            if msg:
                message = msg.message.decode()
                listener_messages.appendleft(message)
        except Exception as e:
            print(f"Listener received error {e}")
            listener_error_count += 1
            last_listener_error = str(e)


@app.route("/")
async def index():
    return await render_template("index.html", client_type="Valkey GLIDE")


@app.route("/publisher_messages")
async def get_publisher_messages():
    return jsonify(list(publisher_messages))


@app.route("/listener_messages")
async def get_listener_messages():
    return jsonify(list(listener_messages))


@app.route("/cluster_nodes")
async def cluster_nodes():
    nodes = await get_cluster_nodes()
    return jsonify({"nodes": nodes})


@app.route("/channel_slot")
async def channel_slot():
    global channels
    slots = []
    for idx, channel in enumerate(channels):
        slot, owner = await get_channel_slot(channel)
        color_class = "message-one" if idx % 2 == 0 else "message-two"
        slots.append(
            {"channel": idx + 1, "slot": slot, "owner": owner, "class": color_class}
        )
    return jsonify({"slots": slots})


async def get_cluster_nodes():
    nodes = await publisher.custom_command(
        ["CLUSTER", "NODES"],
        route=ByAddressRoute("127.0.0.1", app.config["VALKEY_PORT"]),
    )
    nodes = nodes.decode("utf-8")
    parsed_nodes = parse_cluster_nodes(nodes)
    return parsed_nodes


async def get_channel_slot(channel):
    slot = await publisher.custom_command(
        ["CLUSTER", "KEYSLOT", channel],
        route=ByAddressRoute("127.0.0.1", app.config["VALKEY_PORT"]),
    )

    # Retrieve the slot owner's information
    nodes = await publisher.custom_command(
        ["CLUSTER", "NODES"],
        route=ByAddressRoute("127.0.0.1", app.config["VALKEY_PORT"]),
    )
    nodes = nodes.decode("utf-8")

    # Find the node owning this slot
    slot_owner = None
    for node in nodes.strip().split("\n"):
        parts = node.split()
        ip_port = parts[1].split("@")[0]  # Remove the @suffix
        slot_ranges = parts[8:] if len(parts) > 8 else []
        for slot_range in slot_ranges:
            start, end = map(
                int,
                (
                    slot_range.split("-")
                    if "-" in slot_range
                    else (slot_range, slot_range)
                ),
            )
            if start <= int(slot) <= end:
                slot_owner = ip_port
                break
        if slot_owner:
            break

    return (int(slot), slot_owner)


def parse_cluster_nodes(nodes_str):
    nodes = nodes_str.strip().split("\n")
    shards = {}
    failed_nodes = []

    for node in nodes:
        parts = node.split()
        node_id = parts[0]
        ip_port = parts[1].split("@")[0]  # Remove the @suffix
        flags = parts[2].split(",")
        master_id = parts[3]
        slot_ranges = parts[8:] if len(parts) > 8 else []

        shard_id = master_id if master_id != "-" else node_id
        if shard_id not in shards:
            shards[shard_id] = {"slots": [], "primary": "", "replicas": []}

        health = "healthy" if "fail" not in flags else "failed"
        if "master" in flags:
            if health == "failed":
                failed_nodes.append(
                    f'<span class="failed">primary {ip_port} {health}</span>'
                )
            else:
                shards[shard_id]["primary"] = f"{ip_port} {health}"
                shards[shard_id]["slots"].extend(slot_ranges)
        else:
            shards[shard_id]["replicas"].append(
                f'<span class="replica">replica {ip_port} {health}</span>'
            )

    result = []
    shard_index = 1  # Start shard indexing from 1

    for shard_info in shards.values():
        if shard_info["primary"] and "healthy" in shard_info["primary"]:
            slot_str = " ".join(shard_info["slots"])  # Combine all slot ranges
            result.append(
                f"<strong>---<br>Shard {shard_index} Slots {slot_str}</strong>"
            )
            result.append(
                f'<span class="primary">primary {shard_info["primary"]}</span>'
            )
            for replica in shard_info["replicas"]:
                result.append(f"{replica}")
            shard_index += 1  # Increment the shard index for the next shard

    if failed_nodes:
        result.append("<strong>---<br>Failed Nodes</strong>")
        for failed_node in failed_nodes:
            result.append(f"{failed_node}")

    return result


@app.route("/kill_primary_node", methods=["POST"])
async def kill_primary_node():
    request_data = await request.get_json()
    port = request_data.get("port")
    if not port:
        return jsonify({"error": "Port is required"}), 400

    try:
        # Find the process ID of the Redis server running on the specified port with [cluster]
        result = (
            os.popen(f"ps -ef | grep 'redis-server .*:{port} \\[cluster\\]'")
            .read()
            .strip()
        )
        if result:
            lines = result.split("\n")
            for line in lines:
                if f"{port} [cluster]" in line:
                    pid = int(line.split()[1])
                    os.system(f"sudo kill -9 {pid}")
                    return (
                        jsonify({"message": f"Primary node on port {port} killed"}),
                        200,
                    )
            return jsonify({"error": f"No primary node found on port {port}"}), 404
        else:
            return jsonify({"error": f"No primary node found on port {port}"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/error_counts")
async def get_error_counts():
    return jsonify(
        {
            "publisher_errors": publisher_error_count,
            "listener_errors": listener_error_count,
            "last_publisher_error": last_publisher_error,
            "last_listener_error": last_listener_error,
        }
    )


def run_add_shard(port, cluster_folder):
    command = f"python3 ../../utils/cluster_manager.py add_shard --existing-port={port} --replica-count=2 --cluster-folder={cluster_folder}"
    os.system(command)


@app.route("/scale_out", methods=["POST"])
async def scale_out():
    port = app.config["VALKEY_PORT"]
    if not port:
        return jsonify({"error": "Port is required"}), 400

    try:
        # Find the last folder in ../utils/clusters/
        clusters_dir = Path("../../utils/clusters/")
        cluster_folders = sorted(
            clusters_dir.iterdir(), key=lambda x: x.stat().st_mtime
        )
        latest_folder = cluster_folders[-1] if cluster_folders else None
        if not latest_folder:
            return jsonify({"error": "No cluster folder found"}), 404
        absolute_path = os.path.abspath(latest_folder)
        print(f"found cluster folder={absolute_path}")

        # Run the add_shard command in a separate thread
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(run_add_shard, port, absolute_path)
        return (
            jsonify({"message": "Shard addition initiated"}),
            200,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.before_serving
async def before_serving():
    await create_pubsub_clients()
    asyncio.create_task(publish_messages())
    asyncio.create_task(listen_messages())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Valkey Client Demo")
    parser.add_argument("--port", type=int, default=20695, help="Valkey server port")
    args = parser.parse_args()
    app.config["VALKEY_PORT"] = args.port
    app.config["CLIENT_TYPE"] = "Valkey GLIDE"
    app.run(debug=True, port=5000, host="0.0.0.0")
