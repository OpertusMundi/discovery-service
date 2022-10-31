#!/bin/bash

if [ ! -f "./.env" ]; then
    echo "No env file found, copying default"
    cp ./.env-default ./.env
fi