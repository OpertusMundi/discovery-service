import logging
from ast import literal_eval
# Typing
from typing import Dict, List, Optional
from typing_extensions import TypedDict

# Redis
from redis.commands.json.path import Path
from redis.commands.search.query import NumericFilter, Query

from ..clients import redis
from ..utility.typing import Table


def save_celery_task(task_id: str, task_tuple: str) -> None:
    """
    Saves a Celery task as a tuple tree generated from 'as_tuple' in the database under the given task_id.
    """
    redis.get_client().json().set(
        f"task:{task_id}", 
        Path.root_path(),
        {
            "task": {
                "task_tuple": str(task_tuple)
            }
        }
    )


def get_celery_task(task_id: str) -> Optional[tuple]:
    """
    Gets a Celery task tuple tree if the task exists, otherwise returns None.
    """
    query = Query(f"@task_id:{task_id}")
    results = redis.get_client().ft(index_name="task").search(query)
    first = None
    if results.docs:
        first = json.loads(results.docs[0].json)
    return literal_eval(first["task_tuple"]) if first else None


def add_table(table_name: str, table_path: str, table_bucket: str, column_count: int, nodes: Dict[str, str]) -> None:
    """
    Adds a table with some useful metadata to the database.
    """
    redis.get_client().json().set(
        f"table:{table_path}", 
        Path.root_path(),
        {
            "table": {
                "path": table_path,
                "bucket": table_bucket,
                "name": table_name,
                "column_count": column_count,
                "nodes": nodes
            }
        }
    )


def list_tables(bucket=None) -> List[Table]:
    """
    Lists all tables in the given bucket that have metadata (meaning they were ingested).

    If the bucket is 'None', all tables are returned.
    """
    query = Query("*")
    if bucket:
        query = Query(f"@bucket:{bucket}")
    results = redis.get_client().ft(index_name="table").search(query)
    return [json.loads(doc.json) for doc in result.docs]


def get_table(table_path: str) -> Optional[Table]:
    """
    Gets table metadata given the table path, or None if nothing was found.
    """
    query = Query(f"@path:{table_path}")
    results = redis.get_client().ft(index_name="table").search(query)
    first = None
    if results.docs:
        first = json.loads(results.docs[0].json)
    return first


def table_exists(table_path: str) -> bool:
    """
    Checks whether there is any table metadata for the given table path.
    """
    return get_table(table_path) != None


def get_node_ids(table_path: str) -> Dict[str, str]:
    """
    Gets the node ids belonging to the table for the given path.
    """
    res = get_table(table_path)
    if not res:
        logging.warn(f"Could not find metadata for table at {table_path}!")
    return res["nodes"] if res else []


def purge() -> None:
    """
    Purges the database from all data.
    """
    redis.drop_index("table")
    redis.drop_index("task")
    redis.initialize()