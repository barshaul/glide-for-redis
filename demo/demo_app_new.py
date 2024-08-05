import argparse
import asyncio
import logging
import math
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from statistics import mean
from typing import Mapping

import numpy as np
from glide import (
    BaseClientConfiguration,
    ByAddressRoute,
    GlideClusterClient,
    NodeAddress,
    RangeByIndex,
    ReadFrom,
)
from quart import Quart, jsonify, render_template, request

app = Quart(__name__)

# Connect to the Valkey server
valkey_client = None

# Variables to track TPS
transactions = 0
tps = 0
latency_list = []

# Setup logging
logging.basicConfig(level=logging.INFO)

executor = ThreadPoolExecutor(max_workers=10)  # ThreadPoolExecutor for background tasks


# Define multiple leaderboards
# keys_5_shards = ["f", "fbat", "d234", "d23459", "foo", "foo1", "f111"]
hashtags_keys2 = ["f", "fbat", "d234", "foo", "foo1", "f111"]
hashtags_keys = ["f", "d234", "foo1"]

i = 1
leaderboard_keys = []
for key in hashtags_keys:
    leaderboard_keys.append(f"{{{key}}}leaderboard_{i}")
    i += 1

# leaderboard_keys = [
#     f"{{}}leaderboard_{i}" for (i, hashtag) in range(1, len(hashtags_keys) + 1)
# ]  # Adjust the range as needed
# print(leaderboard_keys)


def truncate_decimal(number: float, digits: int = 3) -> float:
    stepper = 10**digits
    return math.floor(number * stepper) / stepper


def calculate_latency(latency_list, percentile):
    return round(np.percentile(np.array(latency_list), percentile), 4)


def latency_results(latencies):
    result = {}
    if latencies:
        result["p50_latency"] = calculate_latency(latencies, 50)
        result["p90_latency"] = calculate_latency(latencies, 90)
        result["p99_latency"] = calculate_latency(latencies, 99)
        result["average_latency"] = truncate_decimal(mean(latencies))
        result["std_dev"] = truncate_decimal(np.std(latencies))
    else:
        result["p50_latency"] = 0
        result["p90_latency"] = 0
        result["p99_latency"] = 0
        result["average_latency"] = 0
        result["std_dev"] = 0
    return result


# Helper functions
async def add_score(user, leaderboard_key):
    global transactions
    while True:
        score = random.randint(1, 100)
        start_time = time.perf_counter()
        try:
            await valkey_client.zadd_incr(leaderboard_key, user, float(score))
        except Exception as e:
            logging.warning(f"recieved exception {e}")
        end_time = time.perf_counter()
        latency_list.append((end_time - start_time) * 1000)
        transactions += 1


async def get_leaderboard(leaderboard_key):
    global transactions
    range_query = RangeByIndex(0, 9)  # Query for the top 10 members
    start_time = time.perf_counter()
    leaderboard = await valkey_client.zrange_withscores(
        leaderboard_key, range_query, reverse=True
    )
    end_time = time.perf_counter()
    latency_list.append((end_time - start_time) * 1000)
    transactions += 1
    return [(user.decode("utf-8"), int(score)) for user, score in leaderboard.items()]


async def calculate_tps():
    global transactions, tps, latency_list
    while True:
        await asyncio.sleep(1)
        tps = transactions
        latency_metrics = latency_results(latency_list)
        logging.info(f"TPS: {tps}, Latency: {latency_metrics}")
        transactions = 0
        latency_list = []


async def get_cluster_nodes():
    nodes = await valkey_client.custom_command(
        ["CLUSTER", "NODES"],
        route=ByAddressRoute("127.0.0.1", app.config["VALKEY_PORT"]),
    )
    nodes = nodes.decode("utf-8")
    parsed_nodes = parse_cluster_nodes(nodes)
    return parsed_nodes


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


def run_add_shard(port, cluster_folder):
    command = f"python3 ../utils/cluster_manager.py add_shard --existing-port={port} --replica-count=2 --cluster-folder={cluster_folder}"
    os.system(command)


@app.route("/add_shard", methods=["POST"])
async def add_shard():
    request_data = await request.get_json()
    port = request_data.get("port")
    if not port:
        return jsonify({"error": "Port is required"}), 400

    try:
        # Find the last folder in ../utils/clusters/
        clusters_dir = Path("../utils/clusters/").resolve()  # Resolve the full path
        cluster_folders = sorted(
            clusters_dir.iterdir(), key=lambda x: x.stat().st_mtime
        )
        latest_folder = cluster_folders[-1] if cluster_folders else None

        if not latest_folder:
            return jsonify({"error": "No cluster folder found"}), 404

        # Run the add_shard command in a separate thread
        executor.submit(run_add_shard, port, latest_folder)
        return (
            jsonify(
                {"message": f"Shard addition initiated for cluster with port {port}"}
            ),
            200,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


async def run_add_scores_continuously():
    while True:
        tasks = [
            add_score(f"user_{i}", leaderboard_keys[i % len(leaderboard_keys)])
            for i in range(1, 1001)
        ]
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.1)  # Adjust sleep time as needed


@app.before_serving
async def before_serving():
    global valkey_client
    config = BaseClientConfiguration(
        [NodeAddress(host="localhost", port=app.config["VALKEY_PORT"])],
        use_tls=False,
        read_from=ReadFrom.PREFER_REPLICA,
    )
    valkey_client = await GlideClusterClient.create(config)
    asyncio.create_task(calculate_tps())
    asyncio.create_task(run_add_scores_continuously())


@app.route("/")
async def index():
    return await render_template("index.html")


@app.route("/leaderboard")
async def leaderboard():
    leaderboards = {}
    for key in leaderboard_keys:
        leaderboards[key.split("}")[1]] = await get_leaderboard(key)
    return jsonify({"leaderboards": leaderboards})


@app.route("/tps")
async def tps_value():
    global latency_list
    latency_metrics = latency_results(latency_list)
    return jsonify({"tps": tps, "latency": latency_metrics})


@app.route("/cluster_nodes")
async def cluster_nodes():
    nodes = await get_cluster_nodes()
    return jsonify({"nodes": nodes})


@app.route("/kill_primary_node", methods=["POST"])
async def kill_primary_node():
    request_data = await request.get_json()
    port = request_data.get("port")
    if not port:
        return jsonify({"error": "Port is required"}), 400

    try:
        # Find the process ID of the Valkey server running on the specified port with [cluster]
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


def run_force_failover(port):
    command = f"redis-cli -p {port} cluster failover force"
    os.system(command)


@app.route("/force_failover", methods=["POST"])
async def force_failover():
    request_data = await request.get_json()
    port = request_data.get("port")
    if not port:
        return jsonify({"error": "Port is required"}), 400

    try:
        # Run the force failover command in a separate thread
        executor.submit(run_force_failover, port)
        return jsonify({"message": f"Failover forced on node with port {port}"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Valkey Client Demo")
    parser.add_argument("--port", type=int, default=20695, help="Valkey server port")
    args = parser.parse_args()
    app.config["VALKEY_PORT"] = args.port
    app.run(debug=True, port=5000, host="0.0.0.0")
