#!/bin/bash

if [ ! -f "./.env" ]; then
    echo "No env file found, copying default"
    cp ./.env-default ./.env
fi

if [ ! -f "./.config" ]; then
    echo "No config file found, copying default"
    cp ./.config-default ./.config
fi