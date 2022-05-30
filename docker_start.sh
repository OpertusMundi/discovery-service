if [ ! -f "./.env" ]; then
    echo "No env file found, copying default"
    cp ./.env-default ./.env
fi
docker-compose down -v --remove-orphans
docker-compose build
docker-compose up