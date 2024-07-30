#!/bin/bash

# Validate the port argument
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <port>"
    exit 1
fi

initial_node_port=$1
initial_node_ip="127.0.0.1"

echo "Fetching cluster info from $initial_node_ip:$initial_node_port..."
cluster_info=$(redis-cli -h $initial_node_ip -p $initial_node_port cluster nodes)

declare -A shard_nodes  # Stores commands by shard

while read -r line; do
    if [[ "$line" == *connected* ]]; then
        node_ip_port=$(echo "$line" | awk '{print $2}' | cut -d '@' -f 1)
        node_role=$(echo "$line" | awk '{print $3}')
        shard_id=$(echo "$line" | awk '{print $7}')

        node_port=$(echo "$node_ip_port" | cut -d ':' -f 2)
        role_label=$(if [[ "$node_role" == "master" ]]; then echo "Primary"; else echo "Replica"; fi)
        command="echo '$role_label: $node_ip_port'; redis-cli -p $node_port info stats | grep instantaneous_ops_per_sec; echo '---';"

        shard_nodes[$shard_id]+="$command "
    fi
done <<< "$cluster_info"

# Construct the watch command
watch_cmd="watch -n 1 'date; echo;"

for shard_id in $(echo "${!shard_nodes[@]}" | tr ' ' '\n' | sort -n); do
    watch_cmd+="echo '--- Shard $shard_id ---'; "
    watch_cmd+="${shard_nodes[$shard_id]}"
    watch_cmd+="echo '--- Shard $shard_id ---'; echo;"
done

watch_cmd+="'"

echo "To monitor all nodes by shard, run the following command:"
echo "$watch_cmd"

# Uncomment to run
# eval "$watch_cmd"

