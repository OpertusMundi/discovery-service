if [ ! -f "./.env" ]; then
    echo "No env file found, copying default"
    cp ./.env-default ./.env
fi
docker-compose down --remove-orphans
docker-compose build --force-rm api
docker-compose build --force-rm metanome
docker-compose build --force-rm celery-worker
docker-compose up