from ..clients import mongodb
from typing import Any, Dict, List, Optional
from typing_extensions import TypedDict
from pymongo.database import Database
from ast import literal_eval

class Table(TypedDict):
    name: str
    column_count: int
    nodes: Dict[str, str]


def get_db() -> Database:
	return mongodb.get_client()["db"]


def save_celery_task(task_id: str, task_tuple: str):
	get_db().celery_tasks.insert_one({"task_id": task_id, "task_tuple": str(task_tuple)})


def get_celery_task(task_id: str) -> Optional[tuple]:
	res = get_db().celery_tasks.find_one({"task_id": task_id})
	return literal_eval(res["task_tuple"]) if res else None


def add_table(table_name: str, table_path: str, table_bucket: str, column_count: int, nodes: Dict[str, str]):
	get_db().table_metadata.insert_one({
		"name": table_name, 
		"path": table_path,
		"bucket": table_bucket,
		"column_count": column_count, 
		"nodes": nodes
	})


def list_tables() -> List[Table]:
	return list(get_db().table_metadata.find())


def get_table(table_path: str) -> Table:
	return get_db().table_metadata.find_one({"path": table_path})


def table_exists(table_path: str) -> bool:
	return get_table(table_path) != None


def get_node_ids(table_path: str) -> Dict[str, str]:
	return get_table(table_path)["nodes"]


def purge():
	get_db().table_metadata.drop()
