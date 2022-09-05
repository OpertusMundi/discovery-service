from backend import celery as celery_app
from celery.result import AsyncResult, GroupResult
import logging


# Based on solution(s)/comments/source in: 
# - https://github.com/celery/celery/issues/4516
# - https://github.com/celery/celery/blob/v4.2.1/celery/canvas.py#L278
# - https://github.com/celery/celery/blob/master/celery/result.py#L933
def generate_status_tree(result):
    if isinstance(result, GroupResult):
        result = celery_app.GroupResult.restore(result.id)

    try:
        result_id = result.id
    except:
        return None

    result_dict = {}

    if isinstance(result, GroupResult):
        result_dict["name"] = "group"
        result_dict["args"] = []
        result_dict["status"] = None

    elif isinstance(result, AsyncResult):
        result_dict["name"] = result.name
        result_dict["args"] = result.args
        result_dict["status"] = result.status

    result_dict["id"] = result_id
    result_dict["parent"] = generate_status_tree(result.parent) if result.parent else None
    result_dict["children"] = [generate_status_tree(child) for child in result.children] if result.children else []

    return result_dict
