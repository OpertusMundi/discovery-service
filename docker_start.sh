#!/bin/bash
source check_config.sh

# Load config
set -o allexport
source .env
set +o allexport

compose_file="docker-compose-dev.yml"
if [ "$DAISY_PRODUCTION" = true ]; then
    compose_file="docker-compose-prod.yml"
fi

docker-compose -f $compose_file build
docker-compose -f $compose_file up --remove-orphans