import argparse
import asyncio
import os
from collections import deque
from datetime import datetime

from glide import (
    ByAddressRoute,
    GlideClientConfiguration,
    GlideClusterClient,
    GlideClusterClientConfiguration,
    NodeAddress,
    PubSubMsg,
)
from quart import Quart, jsonify, render_template, request

app = Quart(__name__)

publisher_messages = deque(maxlen=15)
listener_messages = deque(maxlen=15)
cluster_info = {}

publisher = None
listener = None
channel = "key4290"


async def create_pubsub_clients():
    global channel
    cluster_channels_and_patterns = {
        GlideClusterClientConfiguration.PubSubChannelModes.Exact: {channel}
    }
    pubsub_subscriptions = GlideClusterClientConfiguration.PubSubSubscriptions(
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
    global channel
    while True:
        timestamp = datetime.utcnow().strftime("%H:%M:%S")
        message = f"hello_{timestamp}"
        await publisher.publish(message, channel)
        publisher_messages.appendleft(message)
        await asyncio.sleep(1)


async def listen_messages():
    while True:
        msg = await listener.get_pubsub_message()
        if msg:
            message = msg.message.decode()
            print(f"Recieved message: {message}")
            listener_messages.appendleft(message)


@app.route("/")
async def index():
    return await render_template("index.html")


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
    global channel
    slot = await get_channel_slot(channel)
    return jsonify({"slot": slot})


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
    return slot


def parse_cluster_nodes(nodes_str):
    nodes = nodes_str.strip().split("\n")
    shards = {}

    for node in nodes:
        parts = node.split()
        node_id = parts[0]
        ip_port = parts[1].split("@")[0]  # Remove the @suffix
        flags = parts[2].split(",")
        master_id = parts[3]
        slots = parts[8] if len(parts) > 8 else ""

        shard_id = master_id if master_id != "-" else node_id
        if shard_id not in shards:
            shards[shard_id] = {"slots": slots, "primary": "", "replicas": []}

        health = "healthy" if "fail" not in flags else "failed"
        if "master" in flags:
            shards[shard_id]["primary"] = f"{ip_port} {health}"
            shards[shard_id]["slots"] = slots
        else:
            shards[shard_id]["replicas"].append(f"{ip_port} {health}")

    result = []
    for idx, (shard_id, shard_info) in enumerate(shards.items(), start=1):
        result.append(
            f'<strong>---<br>Shard {idx} Slots {shard_info["slots"]}</strong>'
        )
        result.append(f'<span class="primary">primary {shard_info["primary"]}</span>')
        for replica in shard_info["replicas"]:
            result.append(f'<span class="replica">replica {replica}</span>')

    return result


@app.route("/force_failover", methods=["POST"])
async def force_failover():
    request_data = await request.get_json()
    port = request_data.get("port")
    if not port:
        return jsonify({"error": "Port is required"}), 400

    try:
        command = f"redis-cli -p {port} cluster failover force"
        os.system(command)
        return jsonify({"message": f"Failover forced on node with port {port}"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
    app.run(debug=True, port=5000, host="0.0.0.0")
