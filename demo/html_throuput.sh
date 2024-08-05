#!/bin/bash

initial_node_port=$1
initial_node_ip="127.0.0.1"

# Check if two log files are provided as arguments
if [ "$#" -ne 2 ]; then
    client="Glide"
else
    client=$2
fi
html_file="redis_status_$client.html"
temp_file="temp_status_$client.html"

# Continuously update the dynamic part within the loop
while true; do
    # Begin the dynamic content block
    rm -f $temp_file
    # Create or recreate the HTML file with the static structure
    cat > $temp_file <<- EOF
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="1">
    <title>Cluster Status</title>
    <style>
        body { font-family: Arial, sans-serif; }
        .primary { color: green; }
        .replica { color: red; }
    </style>
</head>
<body>
    <h1>Cluster Status: $client</h1>
EOF

    declare -A shard_data  # Associative array to store shard data
    declare -A shard_slots  # Associative array to store slot ranges for each shard
    declare -A shard_id

    echo "<div>Updated at $(date)</div><table>" >> $temp_file
    cluster_info=$(redis-cli -h $initial_node_ip -p $initial_node_port cluster nodes)
    while read -r line; do
        if [[ "$line" == *connected* ]]; then
            node_ip_port=$(echo "$line" | awk '{print $2}' | cut -d '@' -f 1)
            node_role=$(echo "$line" | awk '{print $3}' | sed 's/myself,//')
            shard_epoch=$(echo "$line" | awk '{print $7}')
            node_port=$(echo "$node_ip_port" | cut -d ':' -f 2)
            stats=$(redis-cli -p $node_port info stats | grep instantaneous_ops_per_sec)
            stats_formatted=$(echo $stats | awk '{print $2}')
            slots=$(echo "$line" | awk '{print $9}')

            role_label=$(if [[ "$node_role" == "master" ]]; then echo "primary"; else echo "replica"; fi)
            shard_data[$shard_epoch]+="<tr class='$role_label'><td>$node_role: $node_ip_port</td><td>$stats</td></tr>"

            # Accumulate unique slots for each shard without duplicates
            if [[ -z "${shard_slots[$shard_epoch]}" ]]; then
                shard_slots[$shard_epoch]="$slots"
            else
                # Prevent duplicating slot range for the shard
                if [[ "${shard_slots[$shard_epoch]}" != *"$slots"* ]]; then
                    shard_slots[$shard_epoch]+=", $slots"
                fi
            fi
        fi
    done <<< "$cluster_info"

    # Output shard data, sorted by shard epoch
    shard_id=0
    for shard_epoch in $(echo ${!shard_data[@]} | tr ' ' '\n' | sort -n); do
        shard_id=$((shard_id + 1))
        slot_ranges=${shard_slots[$shard_epoch]}
        echo "<h2>Shard $shard_id ($slot_ranges)</h2><table>" >> $temp_file
        echo "${shard_data[$shard_epoch]}" >> $temp_file
        echo "</table>" >> $temp_file
    done
    # Finish the dynamic content block
    echo "</table></body></html>" >> $temp_file

    # Append the temporary content to the main file without overwriting the static header
    cat $temp_file > $html_file
    # Clear the associative array for the next iteration
    unset shard_data
    unset shard_slots
    unset shard_id
    sleep 1  # Delay for refresh rate
done
