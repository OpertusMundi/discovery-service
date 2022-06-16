# Daisy - Interactive Data Discovery in Data Lakes

### Requirements 
- Docker

### How to run/use
Run `docker_start.sh`. Then visit the API via `localhost:443` once it is up. There are some built-in routes for testing whether all the containers function, you can check the `src/app.py` file for the specifics.

To make the system start ingesting data, visit the `localhost:443/start` route.

You can edit any python file in the `src` folder with your favorite text editor and it will live-update while the container is running (and in case of the API, restart/reload automatically).

The React front-end can be visited at `localhost:3000`. As with the Python container, this one also supports hot-reloading and editing locally.

The following admin-panels are exposed, for inspecting the services:
- Mongo Express: `localhost:8001`
- Rabbit MQ: `localhost:15672`
- Neo4j: `localhost:7474`
- Elasticsearch: `localhost:5000`
- MinIO server: `localhost:9000`

#### File sharing error
If you get an error about file sharing on windows, visit [this](https://stackoverflow.com/questions/60754297/docker-compose-failed-to-build-filesharing-has-been-cancelled) thread.
