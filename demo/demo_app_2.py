import asyncio
import logging
import math
import random
import time
from multiprocessing import Manager, Process
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
from quart import Quart, jsonify, render_template

app = Quart(__name__)

# Connect to the Redis server
redis_client = None

# Variables to track TPS
manager = Manager()
transactions = manager.Value("i", 0)
tps = manager.Value("i", 0)
latency_list = manager.list()

# Setup logging
logging.basicConfig(level=logging.INFO)


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
async def add_score(user):
    global transactions
    while True:
        score = random.randint(1, 100)
        start_time = time.perf_counter()
        await redis_client.zadd_incr("leaderboard", user, float(score))
        end_time = time.perf_counter()
        latency_list.append((end_time - start_time) * 1000)
        transactions.value += 1


async def add_scores_continuously():
    while True:
        tasks = [add_score(f"user_{i}") for i in range(1, 1001)]
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.1)  # Adjust sleep time as needed


async def get_leaderboard():
    global transactions
    range_query = RangeByIndex(0, 9)  # Query for the top 10 members
    start_time = time.perf_counter()
    leaderboard = await redis_client.zrange_withscores(
        "leaderboard", range_query, reverse=True
    )
    end_time = time.perf_counter()
    latency_list.append((end_time - start_time) * 1000)
    transactions.value += 1
    return [(user.decode("utf-8"), int(score)) for user, score in leaderboard.items()]


async def calculate_tps():
    global transactions, tps, latency_list
    while True:
        await asyncio.sleep(1)
        tps.value = transactions.value
        transactions.value = 0
        latency_metrics = latency_results(latency_list)
        logging.info(f"TPS: {tps.value}, Latency: {latency_metrics}")
        latency_list[:] = []


async def get_cluster_nodes():
    nodes = await redis_client.custom_command(
        ["CLUSTER", "NODES"],
        route=ByAddressRoute("127.0.0.1", 20695),
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


def start_background_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def background_task(loop):
    await redis_client.set_loop(loop)
    await add_scores_continuously()


@app.before_serving
async def before_serving():
    global redis_client
    config = BaseClientConfiguration(
        [NodeAddress(host="localhost", port=20695)],
        use_tls=False,
        read_from=ReadFrom.PREFER_REPLICA,
    )
    redis_client = await GlideClusterClient.create(config)
    asyncio.create_task(calculate_tps())

    new_loop = asyncio.new_event_loop()
    p = Process(target=start_background_loop, args=(new_loop,))
    p.start()
    new_loop.call_soon_threadsafe(asyncio.create_task, background_task(new_loop))


@app.route("/")
async def index():
    return await render_template("index.html")


@app.route("/leaderboard")
async def leaderboard():
    leaderboard = await get_leaderboard()
    return jsonify({"leaderboard": leaderboard})


@app.route("/tps")
async def tps_value():
    global latency_list
    latency_metrics = latency_results(latency_list)
    return jsonify({"tps": tps.value, "latency": latency_metrics})


@app.route("/cluster_nodes")
async def cluster_nodes():
    nodes = await get_cluster_nodes()
    return jsonify({"nodes": nodes})


@app.route("/simulate_topology_change", methods=["POST"])
async def simulate_topology_change():
    # Example of a config change
    # await redis_client.config_set("slave-read-only", "yes")
    return jsonify({"message": "Topology change simulated"})


@app.route("/simulate_scores", methods=["POST"])
async def simulate_scores():
    tasks = [add_score(f"user_{i}") for i in range(1, 1001)]
    await asyncio.gather(*tasks)
    return jsonify({"message": "Scores simulated"})


@app.route("/simulate_and_run", methods=["POST"])
async def simulate_and_run():
    simulate_task = simulate_topology_change()
    score_task = simulate_scores()
    await asyncio.gather(simulate_task, score_task)
    return jsonify({"message": "Simulated topology change and added scores"})


if __name__ == "__main__":
    app.run(debug=True, port=8000, host="0.0.0.0")
