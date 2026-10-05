#!/bin/sh
mkdir -p /app
while :; do redis-server --bind 127.0.0.1 --port 6379 --save '' --appendonly no >/app/redis.log 2>&1; sleep 1; done &
while :; do python3 /app/irc.py; sleep 1; done &
for p in 8000 8001 8002; do
  while :; do
    python3 /app/app.py "$p" &
    pid=$!
    wait "$pid"
    sleep 1
  done &
done
wait
