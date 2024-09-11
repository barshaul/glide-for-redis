# Demo - Getting started
# PubSub Demo

## Run the demo
```
cd demo
./demo_util.sh --run
```

## Stop and cleanup the demo 
```
cd demo
./demo_util.sh --stop
```

## Open Glide's html view in a browser: 
http://<host>:5000


## Open Other's html view in a browser: 
http://<host>:4000


# Throughput Demo
## Create a cluster
```
python3 utils/cluster_manager.py start --cluster-mode -n 3 -r 2
```

## Run the html cluster nodes view
```
cd demo
PORT=<port>
python3 -m http.server 8000 &
./html_throuput.sh $PORT
```

## Open the html view in a browser: 
http://<host>:8000/redis_status.html 

## Run the benchmark
```
cd ../benchmarks/python
CLIENT=glide
python3 python_benchmark.py --resultsFile=../results/new_bench --dataSize 100 --concurrentTasks 100 --clients $CLIENT --host localhost --port $PORT --clientCount 1 --clusterMode
```

## Add shard
```
cd ../..
CLUSTER_FOLDER=/home/ubuntu/glide-for-redis/utils/clusters/redis-cluster-2024-07-30T08-38-48Z-PzHp5m
python3 utils/cluster_manager.py add_shard --existing-port=$PORT --replica-count=2 --cluster-folder=$CLUSTER_FOLDER
```

## Rebalance slots
```
python3 utils/cluster_manager.py rebalance --existing-port=$PORT --cluster-folder=$CLUSTER_FOLDER
```

## Initiate failover
```
REPLICA_PORT=<port>
redis-cli -p $REPLICA_PORT cluster failover force
```
