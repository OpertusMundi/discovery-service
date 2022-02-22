#!/bin/bash

echo "Setting up MinIO client and configuring server..."
mc config host add minio http://minio:9000 minio minio123
mc mb minio/data --ignore-existing
# Disable queue-stuff for now
# mc admin config set minio/ notify_amqp:1 exchange="bucketevents" exchange_type="direct" url="amqp://rabbitmq:5672" routing_key="ingestion_queue"
# mc admin service restart minio/
# sleep 1 # Because MinIO is not actually ready for some magical reason right after the restart command has finished, even though it is not async
# mc event remove minio/data arn:minio:sqs::1:amqp
# mc event add minio/data arn:minio:sqs::1:amqp --event put

if [ $MINIO_CLIENT_MIRROR -eq 1 ]; then
    echo "Mirroring storage folder..."
    mc mirror --exclude ".*" /storage/ minio/data 
fi


echo "Starting modules..."

declare -a modules=(
    "api" 
    # "ingestion_queue" 
)

for path in "${modules[@]}"
do
    bname=$(basename "/src/${path}/${path}.py")
    dname=$(dirname "/src/${path}/${path}.py")
    stem=$(echo $bname | cut -d. -f1)
    echo Starting $stem...
    screen -L -Logfile "/logs/${bname}.log" -dm -S $stem python3 -m src.${path}.${path}
done

screen -r api