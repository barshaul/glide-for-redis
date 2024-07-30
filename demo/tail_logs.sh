#!/bin/bash

# Check if two log files are provided as arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <log_file1> <log_file2>"
    exit 1
fi

# Assign log files from arguments
log_file1=$1
log_file2=$2

# Setup a trap to handle SIGINT (CTRL+C), and cleanup
trap 'cleanup' SIGINT

# Function to cleanup before exiting
cleanup() {
    echo "Interrupt received, stopping..."
    # Ensure to kill background processes if still running
    kill $pid1 $pid2 2>/dev/null
    exit 0
}

# Function to follow logs, search for "won", and print logs
function follow_and_search {
    local log_path="$1"
    tail -f "$log_path" | tee /dev/tty | grep --line-buffered "won" &
    local pid=$!
    echo $pid
}

# Start following both logs
pid1=$(follow_and_search "$log_file1")
pid2=$(follow_and_search "$log_file2")

# Wait for either grep to succeed
if wait $pid1; then
    winner_log=$log_file1
elif wait $pid2; then
    winner_log=$log_file2
fi

# Once one grep finds "won", kill the other tail process and finish
kill $pid1 $pid2 2>/dev/null

# Extract port from the log file path
port=$(echo $winner_log | grep -oP '(?<=/)\d+(?=/redis.log)')

# Get current date and time with milliseconds
current_time=$(date "+%Y-%m-%d %H:%M:%S.%3N")

# Final message
echo "Failover was completed at $current_time! Replica $port won the elections and promoted to a primary."

# Optionally, exit the script or do additional cleanup
exit 0

