#!/bin/bash

log_file="/home/ubuntu/glide-for-redis/benchmarks/python/glide-logs/glide-demo.2024-07-29-18"
log_data_file="/home/ubuntu/glide-for-redis/demo/log_data.html"
# Continuously update the log data file
tail -f $log_file | while read line; do
    # Remove ANSI color codes and format for HTML
    line=$(echo "$line" | sed -r "s/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[m|K]//g")
    echo "$line<br>" >> $log_data_file
done
