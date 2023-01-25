# Discovery service 

[![Build Status](https://ci.dev-1.opertusmundi.eu:9443/api/badges/OpertusMundi/discovery-service/status.svg?ref=refs/heads/main)](https://ci.dev-1.opertusmundi.eu:9443/OpertusMundi/discovery-service)

#### Part of the value-added services suite for Topio

The discovery service is an application for dataset discovery with 3 components:
1. It exposes a series of services via `REST API`
2. It automatically ingests newly added datasets using a `scheduler` implemented with `Celery`.
The scheduler can be configured using **DATA_INGESTION_INTERVAL** variable in `.env-default`.
The default value is `60` **seconds**.
3. It provides services for Jupyter Notebook via a developed plugin: https://github.com/Archer6621/jupyterlab-daisy 

## Requirements
The entire project is containerised, therefore the only requirement is `Docker`

## API Documentation

You can browse the full [OpenAPI documentation](https://opertusmundi.github.io/discovery-service/).

## How to run/use
The discovery service is available for both development and production. 


### Environment variables

The environement varibles can be found in `.env-default`
> Always delete the auto-generated `.env` file **after** changing something in `.env-default`

- `DAISY_PRODUCTION` - `TRUE` to run in production mode and `FALSE` to run in development mode. **Default** FALSE
- `DATA_INGESTION_INTERVAL` - The time interval in SECONDS for starting the auto-ingest pipeline. 
The time interval should reflect how often new data is uploaded/received. 
- `DATA_ROOT_PATH` - The location of the datasets 


### Running

Run `docker_start.sh` to start Docker. Based on the **DAISY_PRODUCTION** variable, it will automatically use
the appropriate docker-compose. 

Visit the API Documentation via `localhost:443` once the application is up.

### Steps to ingest data:

1. Run `/ingest-data` endpoint.
   1. The data should be in the `data` folder and it has to follow this structure:
      `{id}/resources/{file-name}.csv`
   2. This endpoint will take a while to run. The more data to process, the more it will run.
2. Run `/profile-metanome` endpoint. Blocker task. 
   1. This endpoint will take a while to run. The more data to process, the more it will run.
3. Run `/filter-connections` to remove extra edges. 


### To remove all the data:
1. Run `/purge`. This will remove all the data from neo4j and redis. 


#### Use the discovery service:
1. Get joinable tables - Get all assets that share a column(key) with the speficied asset
`/get-joinable` with input: `asset_id`
2. Get related assets - Given a source and a target, show how and if the assets are connected
`/get-realted` with 2 input variables `from_asset_id` and `to_asset_id`


### Monitoring
(Development) The following admin-panels are exposed, for inspecting the services:

- Rabbit MQ: `localhost:15672`
- Neo4j: `localhost:7474`
- Celery Flower: `localhost:5555`
- Redis: `localhost:8001`
- Metanome: `localhost: 8080`


## Development 
You can edit any python file in the `src` folder with your favorite text editor and it will live-update while the container is running (and in case of the API, restart/reload automatically).


#### File sharing error
If you get an error about file sharing on windows, visit [this](https://stackoverflow.com/questions/60754297/docker-compose-failed-to-build-filesharing-has-been-cancelled) thread.
