import argparse
import os
import time
from collections import deque
from datetime import datetime, timezone

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
channel = "key4"


def create_pubsub_clients():
    global channel
    global global_client
    global listener
    global publisher
    port = app.config["VALKEY_PORT"]
    global_client = RedisCluster(host="localhost", port=port, decode_responses=True)
    publisher = RedisCluster(host="localhost", port=port)
    listener = RedisCluster(host="localhost", port=port, decode_responses=True).pubsub()
    listener.subscribe(channel)
    return publisher, listener


def publish_messages():
    global channel
    while True:
        try:
            timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
            message = f"hello_{timestamp}"
            publisher.publish(channel, message)
            publisher_messages.appendleft(message)
        except Exception as e:
            print(f"Publisher eceived error {e}")
        time.sleep(1)


def listen_messages():
    while True:
        try:
            msg = listener.get_message(ignore_subscribe_messages=True, timeout=None)
            if msg:
                message = msg["data"]
                listener_messages.appendleft(message)
        except Exception as e:
            print(f"Listener received error {e}")


@app.route("/")
async def index():
    return await render_template("index.html", client_type="Other")


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
    global channel
    slot = get_channel_slot(channel)
    return jsonify({"slot": slot})


def get_cluster_nodes():
    nodes = global_client.execute_command("CLUSTER NODES")
    parsed_nodes = parse_cluster_nodes(nodes)
    return parsed_nodes


def get_channel_slot(channel):
    slot = global_client.execute_command("CLUSTER KEYSLOT", channel)
    return slot


def parse_cluster_nodes(nodes_dict):
    shards = {}
    failed_nodes = []

    for ip_port, node_info in nodes_dict.items():
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
                failed_nodes.append(f'<span class="failed">{ip_port} {health}</span>')
            else:
                shards[shard_id]["primary"] = f"{ip_port} {health}"
                shards[shard_id]["slots"] = slots
        else:
            shards[shard_id]["replicas"].append(
                f'<span class="replica">{ip_port} {health}</span>'
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Valkey Client Demo")
    parser.add_argument("--port", type=int, default=20695, help="Valkey server port")
    parser.add_argument("--client", type=str, default="Other", help="The client type")
    args = parser.parse_args()
    app.config["VALKEY_PORT"] = args.port
    app.config["CLIENT_TYPE"] = args.client
    app.run(debug=True, port=4000, host="0.0.0.0")
