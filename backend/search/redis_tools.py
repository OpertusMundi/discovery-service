import logging
import json
import hashlib

from ast import literal_eval
# Typing
from typing import Dict, List, Optional

# Redis
from redis.commands.json.path import Path
from redis.commands.search.query import Query

from ..clients import redis
from ..utility.typing import Table


def _deterministic_hash(string: str) -> str:
    return hashlib.sha256(str.encode(string)).hexdigest()


def save_celery_task(task_id: str, task_tuple: str) -> None:
    """
    Saves a Celery task as a tuple tree generated from 'as_tuple' in the database under the given task_id.
    """
    task_id_hash = _deterministic_hash(task_id)
    redis.get_client().json().set(
        task_id_hash,
        Path.root_path(),
        {
            "task": {
                "id": task_id_hash,
                "name": task_id,
                "task_tuple": str(task_tuple)
            }
        }
    )


def get_celery_task(task_id: str) -> Optional[tuple]:
    """
    Gets a Celery task tuple tree if the task exists, otherwise returns None.
    """
    query = Query(f"@id:{_deterministic_hash(task_id)}")
    res = redis.get_client().ft(index_name="task").search(query)
    first = None
    if res.docs:
        first = json.loads(res.docs[0].json)
    return literal_eval(first["task"]["task_tuple"]) if first else None


def add_table(table_name: str, table_path: str, column_count: int, nodes: Dict[str, str]) -> None:
    """
    Adds a table with some useful metadata to the database.
    """
    table_path_hash = _deterministic_hash(table_path)
    redis.get_client().json().set(
        f"table:{table_path_hash}",
        Path.root_path(),
        {
            "table": {
                "id": table_path_hash,
                "path": table_path,
                "name": table_name,
                "column_count": column_count,
                "nodes": nodes
            }
        }
    )


def list_tables() -> List[Table]:
    """
    Lists all tables that have metadata (meaning they were ingested).
    """
    query = Query("*")
    res = redis.get_client().ft(index_name="table").search(query)
    return [json.loads(doc.json)['table'] for doc in res.docs]


def get_table(table_path: str) -> Optional[Table]:
    """
    Gets table metadata given the table path, or None if nothing was found.
    """
    table_path_hash = _deterministic_hash(table_path)
    query = Query(f"@id:{table_path_hash}")
    res = redis.get_client().ft(index_name="table").search(query)
    first = None
    if res.docs:
        first = json.loads(res.docs[0].json)
    return first['table'] if first else None


def table_exists(table_path: str) -> bool:
    """
    Checks whether there is any table metadata for the given table path.
    """
    return get_table(table_path) is not None


def get_node_ids(table_path: str) -> Dict[str, str]:
    """
    Gets the node ids belonging to the table for the given path.
    """
    res = get_table(table_path)
    if not res:
        logging.warning(f"Could not find metadata for table at '{table_path}'!")
    return res["nodes"] if res else {}


def purge() -> None:
    """
    Purges the database from all data.
    """
    redis.drop_index("table")
    redis.drop_index("task")
    redis.initialize()
