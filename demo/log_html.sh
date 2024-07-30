#!/bin/bash

log_file="/home/ubuntu/glide-for-redis/benchmarks/python/glide-logs/glide-demo.2024-07-30-07"
html_file="/home/ubuntu/glide-for-redis/demo/log.html"
# Create initial HTML structure
cat > $html_file <<- EOF
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="1">
    <title>Log File Viewer</title>
    <style>
        body { font-family: monospace; }
        .info { color: green; }
        .debug { color: blue; }
        .error { color: red; }
    </style>
    <script>
        // Function to automatically scroll to the bottom of the page
        function updateScroll() {
            var element = document.documentElement;
            var bottom = element.scrollHeight - element.clientHeight;
            window.scrollTo(0, bottom);
        }
        // Execute on page load
        # window.onload = updateScroll;
    </script>
</head>
<body>
    <h1>Log File Output</h1>
    <pre id="log">
EOF

# Continuously update the log part of the HTML file
tail -f $log_file | while read line; do
    # Remove ANSI color codes
    line=$(echo "$line" | sed -r "s/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[m|K]//g")
    # Apply HTML color based on log level
    if echo "$line" | grep -q "INFO"; then
        echo "<span class='info'>$line</span><br>" >> $html_file
    elif echo "$line" | grep -q "DEBUG"; then
        echo "<span class='debug'>$line</span><br>" >> $html_file
    elif echo "$line" | grep -q "ERROR"; then
        echo "<span class='error'>$line</span><br>" >> $html_file
    else
        echo "$line<br>" >> $html_file
    fi
    # Always close the HTML tags properly
    echo "</pre></body></html>" >> $html_file
done
