#!/bin/bash
cd /metanome
screen -L -Logfile "/logs/metanome_backend.log" -dm -S metanome bash run.sh

cd /src
screen -L -Logfile "/logs/metanome_api.log" -dm -S api python3 api.py

screen -R api