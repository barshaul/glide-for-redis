#!/bin/bash

# Function to create two new clusters and parse the output
create_clusters_and_get_ports() {
    echo "Creating the first cluster..."
    cluster_output_1=$(python3 /home/ubuntu/glide-for-redis/utils/cluster_manager.py start --cluster-mode -n 3 -r 2)
    echo "$cluster_output_1"

    echo "Creating the second cluster..."
    cluster_output_2=$(python3 /home/ubuntu/glide-for-redis/utils/cluster_manager.py start --cluster-mode -n 3 -r 2)
    echo "$cluster_output_2"

    # Extract CLUSTER_NODES from both outputs
    cluster_nodes_1=$(echo "$cluster_output_1" | grep "CLUSTER_NODES" | cut -d'=' -f2)
    cluster_nodes_2=$(echo "$cluster_output_2" | grep "CLUSTER_NODES" | cut -d'=' -f2)

    # Get the first slave port from both clusters
    PORT_GLIDE=$(get_slave_port "$cluster_nodes_1")
    PORT_OTHER=$(get_slave_port "$cluster_nodes_2")

    if [ -z "$PORT_GLIDE" ] || [ -z "$PORT_OTHER" ]; then
        echo "Error: Could not find a slave port in one of the clusters."
        exit 1
    fi

    echo "Found slave ports: PORT_GLIDE=$PORT_GLIDE, PORT_OTHER=$PORT_OTHER"
}

# Function to find a slave port from a list of nodes
get_slave_port() {
    local cluster_nodes=$1

    IFS=',' read -r -a node_array <<< "$cluster_nodes"
    for node in "${node_array[@]}"; do
        port=$(echo "$node" | cut -d':' -f2)
        role=$(redis-cli -p "$port" info replication | grep "role:slave")
        if [ -n "$role" ]; then
            echo "$port"
            return
        fi
    done

    echo "" # Return an empty string if no slave is found
}

# Function to start the processes
run() {
    # Create clusters and get the slave ports
    create_clusters_and_get_ports

    echo "Activating virtual environment..."
    source /home/ubuntu/glide-for-redis/python/.env/bin/activate

    echo "Running pubsub_demo.py on port $PORT_GLIDE"
    python3 /home/ubuntu/glide-for-redis/demo/pubsub_demo/pubsub_demo.py --port "$PORT_GLIDE" > /dev/null 2>&1 &

    echo "Running pubsub_demo_sync.py on port $PORT_OTHER"
    python3 /home/ubuntu/glide-for-redis/demo/pubsub_demo_sync/pubsub_demo_sync.py --port "$PORT_OTHER" > /dev/null 2>&1 &
}

# Function to stop the processes
stop() {
    echo "Stopping pubsub_demo.py and pubsub_demo_sync.py..."

    # Loop to kill processes until they are no longer running
    while pgrep -f "python.*pubsub_demo.py" > /dev/null || pgrep -f "python.*pubsub_demo_sync.py" > /dev/null; do
        pkill -f "python.*pubsub_demo.py"
        pkill -f "python.*pubsub_demo_sync.py"
        echo "Waiting for processes to terminate..."
        sleep 1
    done

    echo "Processes terminated."

    echo "Killing all Redis servers..."
    sudo pkill -9 redis-server

    echo "Cleaning up cluster folders..."
    rm -rf /home/ubuntu/glide-for-redis/utils/clusters/
}

# Main script logic
if [ "$1" == "--run" ]; then
    stop
    run
elif [ "$1" == "--stop" ]; then
    stop
else
    echo "Usage: $0 --run or $0 --stop"
    exit 1
fi
