import os

from redis import Redis

from ..utility.parsing import parse_ip

redis_client: Redis = None

def get_client() -> Redis:
    global redis_client

    if redis_client is None:
        host, port = parse_ip(os.environ["REDIS_ADDRESS"])
        redis_client = Redis(host=host, port=port, db=0)

    return redis_client
