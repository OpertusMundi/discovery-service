#!/bin/bash

if [ ! "$DAISY_PRODUCTION" = true ]; then
    echo "Setting up MinIO client and configuring server..."
    mc config host add minio http://minio:9000 minio minio123
    mc mb minio/data --ignore-existing
fi

echo "Starting app..."
screen -L -Logfile "/backend/logs/app.py.log" -dm -S app python3 -m backend.app

screen -r app

