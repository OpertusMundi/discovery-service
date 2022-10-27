import os
import logging

from redis import StrictRedis
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.exceptions import ResponseError

from ..utility.parsing import parse_ip

redis_client: StrictRedis = None



def get_client() -> StrictRedis:
    global redis_client
    if redis_client is None:
        redis_client = StrictRedis(host=os.environ["REDIS_HOST"], port=os.environ["REDIS_PORT"], db=0, password=os.environ["REDIS_PASSWORD"])
        initialize()
    return redis_client


def initialize():
    table_schema = (
        TagField("$.table.path", as_name="path"),
        TagField("$.table.bucket", as_name="bucket")
    
    )
    task_schema = (
        TagField("$.task.id", as_name="id"),
    )
    _setup_index("table", table_schema)
    _setup_index("task", task_schema)


def drop_index(name):
    get_client().ft(index_name=name).dropindex(delete_documents=True)


def _setup_index(name, schema):
    try:
        get_client().ft(index_name=name).create_index(
            schema, 
            definition=IndexDefinition(prefix=[f"{name}:"], index_type=IndexType.JSON)
        )
    except ResponseError:
        logging.warn(f"index '{name}' already exists")