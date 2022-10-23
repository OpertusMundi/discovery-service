#!/bin/sh
if [ ! -f "./.env" ]; then
    echo "No env file found, copying default"
    cp ./.env-default ./.env
fi

if [ ! -f "./.config" ]; then
    echo "No config file found, copying default"
    cp ./.config-default ./.config
fi

# Load config
set -o allexport
source .config
set +o allexport

compose_file="docker-compose-dev.yml"
if [ "$DAISY_PRODUCTION" = true ]; then
    compose_file="docker-compose-prod.yml"
fi

if [ "$DAISY_PURGE" = true ] ; then
    echo "Purging data..."
    docker-compose -f $compose_file down -v --remove-orphans
fi

docker-compose -f $compose_file build
docker-compose -f $compose_file up