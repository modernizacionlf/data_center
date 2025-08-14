#!/bin/bash

# Print startup message
echo "Starting Data Center API with Scheduler..."
echo "Current time: $(date)"

# Start cron daemon in background
service cron start
echo "Cron daemon started"

# Print loaded cron jobs
echo "Loaded cron jobs:"
crontab -l

# Create log file if it doesn't exist
touch /var/log/datacenter-job.log

# Start FastAPI in foreground
echo "Starting FastAPI server..."
exec fastapi dev --host 0.0.0.0 --port 8002 /api/main.py
