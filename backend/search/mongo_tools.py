import logging
from ast import literal_eval
# Typing
from typing import Dict, List, Optional

from pymongo.database import Database

from ..clients import mongodb
from ..utility.typing import Table


def get_db() -> Database:
    """
    Gets the database object.
    """
    return mongodb.get_client()["db"]


def save_celery_task(task_id: str, task_tuple: str) -> None:
    """
    Saves a Celery task as a tuple tree generated from 'as_tuple' in the database under the given task_id.
    """
    get_db().celery_tasks.insert_one({"task_id": task_id, "task_tuple": str(task_tuple)})


def get_celery_task(task_id: str) -> Optional[tuple]:
    """
    Gets a Celery task tuple tree if the task exists, otherwise returns None.
    """
    res = get_db().celery_tasks.find_one({"task_id": task_id})
    return literal_eval(res["task_tuple"]) if res else None


def add_table(table_name: str, table_path: str, column_count: int, nodes: Dict[str, str]) -> None:
    """
    Adds a table with some useful metadata to the database.
    """
    get_db().table_metadata.insert_one({
        "name": table_name,
        "path": table_path,
        "column_count": column_count,
        "nodes": nodes
    })


def list_tables() -> List[Table]:
    """
    Lists all tables that have metadata (meaning they were ingested).

    """
    return list(get_db().table_metadata.find())


def get_table(table_path: str) -> Optional[Table]:
    """
    Gets table metadata given the table path, or None if nothing was found.
    """
    return get_db().table_metadata.find_one({"path": table_path})


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
        logging.warn(f"Could not find metadata for table at {table_path}!")
    return res["nodes"] if res else []


def purge() -> None:
    """
    Purges the database from all data.
    """
    get_db().table_metadata.drop()
    get_db().celery_tasks.drop()
