import argparse
import os
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import redis
from quart import Quart, jsonify, render_template, request
from redis.cluster import RedisCluster

app = Quart(__name__)

publisher_messages = deque()
listener_messages = deque()
cluster_info = {}

publisher = None
listener = None
global_client = None
channels = ["foo", "foo2"]
# Global error counters
publisher_error_count = 0
listener_error_count = 0
last_publisher_error = ""
last_listener_error = ""


def create_pubsub_clients():
    global channels
    global global_client
    global listener
    global publisher
    port = app.config["VALKEY_PORT"]
    global_client = RedisCluster(host="localhost", port=port, decode_responses=True)
    publisher = RedisCluster(host="localhost", port=port)
    listener = RedisCluster(host="localhost", port=port, decode_responses=True).pubsub()
    listener.subscribe(channels)
    return publisher, listener


def publish_messages():
    global channels, publisher_error_count, last_publisher_error
    while True:
        try:
            for idx, channel in enumerate(channels):
                timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
                message = f"channel_{idx+1}_{timestamp}"
                publisher.publish(channel, message)
                publisher_messages.appendleft(message)
        except Exception as e:
            print(f"Publisher eceived error {e}")
            publisher_error_count += 1
            last_publisher_error = str(e)
        time.sleep(1)


def create_new_subscriber():
    global new_listener, listener, listener_error_count
    while True:
        try:
            new_listener = RedisCluster(
                host="localhost",
                port=app.config["VALKEY_PORT"],
                decode_responses=True,
            ).pubsub()
            new_listener.subscribe(channels)
            listener = new_listener
            return
        except Exception:
            listener_error_count += 1
            time.sleep(0.5)


def listen_messages():
    global listener, listener_error_count, last_listener_error
    while True:
        try:
            msg = listener.get_message(
                ignore_subscribe_messages=True,
                timeout=None,
            )
            if msg:
                message = msg["data"]
                listener_messages.appendleft(message)
        except Exception as e:
            print(f"Listener received error {e}")
            listener_error_count += 1
            last_listener_error = str(e)
            create_new_subscriber()


@app.route("/")
async def index():
    return await render_template("index.html", client_type="Other")


def run_add_shard(port, cluster_folder):
    command = f"python3 ../../utils/cluster_manager.py add_shard --existing-port={port} --replica-count=2 --cluster-folder={cluster_folder}"
    os.system(command)


@app.route("/scale_out", methods=["POST"])
def scale_out():
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


@app.route("/publisher_messages")
async def get_publisher_messages():
    return jsonify(list(publisher_messages))


@app.route("/listener_messages")
async def get_listener_messages():
    return jsonify(list(listener_messages))


@app.route("/cluster_nodes")
async def cluster_nodes():
    nodes = get_cluster_nodes()
    return jsonify({"nodes": nodes})


@app.route("/channel_slot")
async def channel_slot():
    global channels
    slots = []
    for idx, channel in enumerate(channels):
        slot, owner = get_channel_slot(channel)
        color_class = "message-one" if idx % 2 == 0 else "message-two"
        slots.append(
            {"channel": idx + 1, "slot": slot, "owner": owner, "class": color_class}
        )
    return jsonify({"slots": slots})


def get_cluster_nodes():
    nodes = global_client.execute_command("CLUSTER NODES")
    parsed_nodes = parse_cluster_nodes(nodes)
    return parsed_nodes


def get_channel_slot(channel):
    slot = global_client.execute_command("CLUSTER KEYSLOT", channel)
    # Retrieve the slot owner's information
    nodes = global_client.execute_command(
        "CLUSTER NODES",
        target_nodes=redis.cluster.ClusterNode("127.0.0.1", app.config["VALKEY_PORT"]),
    )
    # Find the node owning this slot
    for address, data in nodes.items():
        if "slots" in data:
            for slot_range in data["slots"]:
                if int(slot_range[0]) <= slot <= int(slot_range[1]):
                    return (str(slot), str(address))
    return None


def generate_made_up_ip(ip_map, ip_port, start_ip):
    # Split out the port from the ip_port
    ip, port = ip_port.split(":")

    # If this IP has already been mapped, return the mapped IP
    if ip in ip_map:
        return f"{ip_map[ip]}:{port}"

    # Otherwise, map it to the next IP in the subnet
    new_ip = f"192.168.1.{start_ip + len(ip_map)}"
    ip_map[ip] = new_ip

    return f"{new_ip}:{port}"


def parse_cluster_nodes(nodes_dict):
    shards = {}
    failed_nodes = []
    ip_map = {}  # To store mappings of original IPs to made-up IPs
    start_ip = 100  # Starting number for the last part of the IP (e.g., 192.168.1.100)

    for ip_port, node_info in nodes_dict.items():
        # Generate a made-up IP
        made_up_ip_port = generate_made_up_ip(ip_map, ip_port, start_ip)

        node_id = node_info["node_id"]
        flags = node_info["flags"].split(",")
        master_id = node_info["master_id"]
        slots = node_info["slots"]

        shard_id = master_id if master_id != "-" else node_id
        if shard_id not in shards:
            shards[shard_id] = {"slots": slots, "primary": "", "replicas": []}

        health = "healthy" if node_info["connected"] else "failed"
        if "master" in flags or "myself,master" in flags:
            if health == "failed":
                failed_nodes.append(
                    f'<span class="failed">{made_up_ip_port} {health}</span>'
                )

            else:
                shards[shard_id]["primary"] = f"{made_up_ip_port} {health}"
                shards[shard_id]["slots"] = slots
        else:
            shards[shard_id]["replicas"].append(
                f'<span class="replica">{made_up_ip_port} {health}</span>'
            )

    result = []
    for idx, (shard_id, shard_info) in enumerate(shards.items(), start=1):
        if shard_info["primary"] and "healthy" in shard_info["primary"]:
            slots = (
                ", ".join([f"{slot[0]}-{slot[1]}" for slot in shard_info["slots"]])
                if shard_info["slots"]
                else "None"
            )
            result.append(f"<strong>---<br>Shard {idx} Slots {slots}</strong>")
            result.append(
                f'<span class="primary">primary {shard_info["primary"]}</span>'
            )
            for replica in shard_info["replicas"]:
                result.append(f'<span class="replica">replica {replica}</span>')

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


@app.before_serving
async def before_serving():
    create_pubsub_clients()
    app.add_background_task(publish_messages)
    app.add_background_task(listen_messages)


@app.route("/error_counts")
async def get_error_counts():
    global listener_error_count, publisher_error_count, last_publisher_error, last_listener_error
    return jsonify(
        {
            "publisher_errors": publisher_error_count,
            "listener_errors": listener_error_count,
            "last_publisher_error": last_publisher_error,
            "last_listener_error": last_listener_error,
        }
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Valkey Client Demo")
    parser.add_argument("--port", type=int, default=20695, help="Valkey server port")
    parser.add_argument("--client", type=str, default="Other", help="The client type")
    args = parser.parse_args()
    app.config["VALKEY_PORT"] = args.port
    app.config["CLIENT_TYPE"] = args.client
    app.run(debug=True, port=3000, host="0.0.0.0")
