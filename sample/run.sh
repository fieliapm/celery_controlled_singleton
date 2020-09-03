#!/bin/bash

for i in {1..3}; do
    celery -A tasks worker --loglevel=INFO --concurrency=1 -n "worker$i@%h" &
done

sleep 5

./service.py

pkill -f 'celery -A tasks worker'
