#!/bin/bash
# PCRDB Restart Script

# Kill existing run.py process
pids=$(pgrep -f "python3 run.py")

if [ -n "$pids" ]; then
    echo "Stopping existing process... PIDs: $pids"
    kill $pids
    sleep 2
else
    echo "No existing process found."
fi

# Start new process
echo "Starting pcrdb..."
nohup python3 run.py > server.log 2>&1 &
echo "Server started! Logs: server.log"
