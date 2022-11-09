#!/bin/bash

# This is used to copy over data for development purposes
# Make sure to put your tables in ./data
docker container create --name temp -v discovery-service-fork_data:/data hello-world
docker cp ./data temp:/
docker rm temp
