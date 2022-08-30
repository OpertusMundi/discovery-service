from ..clients import mongodb
from typing import Any
from typing_extensions import TypedDict
from pymongo.database import Database


class Table(TypedDict):
    name: str
    column_count: int
    nodes: dict[str, str]


def get_db() -> Database:
	return mongodb.get_client()["db"]


def store_celery_task_id(parent_id: str, task_id: str):
	get_db().celery_tasks.insert_one({"parent_id": parent_id, "task_id": task_id})


def get_celery_task_id(parent_id: str) -> str:
	res = get_db().celery_tasks.find_one({"parent_id": parent_id})
	return res["task_id"] if res else None


def add_table(table_name: str, column_count: int, nodes: dict[str, str]):
	get_db().table_metadata.insert_one({"name": table_name, "column_count": column_count, "nodes": nodes})


def list_tables() -> list[Table]:
	return list(get_db().table_metadata.find())


def get_table(table_name: str) -> Table:
	return get_db().table_metadata.find_one({"name": table_name})


def get_node_ids(table_name: str) -> dict[str, str]:
	return get_table(table_name)["nodes"]


def purge():
	get_db().table_metadata.drop()
