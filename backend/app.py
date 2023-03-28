#!/usr/bin/env python

import json
import logging

from celery import chord, group
from celery.result import result_from_tuple
from flask import Response
from flask import request
from flask_restx import Resource, fields

from backend import api
# Import own modules
from backend import app
from backend.discovery.queries import delete_spurious_connections, get_related_between_two_tables
from backend.utility.celery_tasks import *
from backend.utility.celery_utils import generate_status_tree
from backend.utility.display import log_format
from backend.search import redis_tools as db

# Display/logging settings
logging.basicConfig(format=log_format, level=logging.INFO)

TaskIdModel = api.model('TaskId', {'task_id': fields.String, 'type': fields.String})


@api.route('/ingest-data')
@api.doc(description="Ingest all the data present in the data volume.")
class IngestData(Resource):
    @api.response(202, 'Success, processing in backend', TaskIdModel)
    @api.response(204, 'No data on volume')
    def get(self):
        paths = search.io_tools.get_tables()
        if len(paths) == 0:
            return Response("No data present on volume", status=204)

        header = []
        for table_path in paths:
            if not db.table_exists(table_path):
                header.append(add_table.s(table_path))
            else:
                logging.info(f"Table {table_path} was already processed!")

        task_group = group(*header)
        profiling_chord = chord(task_group)(profile_valentine_all.si() | find_inds_all.si())
        profiling_chord.parent.save()
        db.save_celery_task(profiling_chord.id, profiling_chord.as_tuple())

        return Response(json.dumps({"task_id": profiling_chord.id, "type": "Ingestion"}), mimetype='application/json',
                        status=202)


# Recursive model spec broken, is fixed in next version of flask_restx, but no idea when it will release
# Sources:
# - https://github.com/python-restx/flask-restx/pull/174
# - https://github.com/python-restx/flask-restx/issues/211
TaskStatusModel = api.model("TaskStatus", {
    'name': fields.String,
    'args': fields.String,
    'status': fields.String,
    'id': fields.String
})
TaskStatusModel["children"] = fields.List(fields.Nested(TaskStatusModel), default=[])
TaskStatusModel["parent"] = fields.Nested(TaskStatusModel)


@api.route('/task-status')
@api.doc(description="Checks the status of a task.")
@api.doc(params={
    'task_id': {'description': 'ID of task to check', 'in': 'query', 'type': 'string', 'required': 'true'},
})
class TaskStatus(Resource):
    @api.response(200, 'Success', TaskStatusModel)
    @api.response(400, 'Missing task id query parameter')
    @api.response(404, 'Task does not exist')
    @api.response(500, 'Task could not be loaded from backend')
    def get(self):
        task_id = request.args.get("task_id")
        if not task_id:
            return Response("Missing task id query parameter", status=400)

        result_tuple = db.get_celery_task(task_id)
        if not result_tuple:
            return Response("Task does not exist", status=404)

        result = result_from_tuple(result_tuple)
        if result is None:
            return Response("Task could not be loaded from backend", status=500)

        return Response(json.dumps(generate_status_tree(result)), mimetype='application/json', status=200)


@api.route('/purge')
@api.doc(description="Purges all of the databases.")
class Purge(Resource):
    @api.response(200, 'Success')
    def get(self):
        db.purge()
        discovery.crud.delete_all_nodes()
        return Response('Success', status=200)


@api.route('/filter-connections')
@api.doc(description="Filters spurious connections. This step is required after the ingestion phase.")
class FilterConnections(Resource):
    # NOTE: We can only have a single response per code, see: https://github.com/python-restx/flask-restx/issues/274
    @api.response(200, 'Success', api.model('DeletedRelations', {
        'deleted_relations': fields.List(fields.String)
    }))
    def get(self):
        deleted_relations = delete_spurious_connections()

        if not deleted_relations:
            logging.info("No relations have been deleted")

        return Response(json.dumps({"deleted_relations": deleted_relations}), mimetype='application/json', status=200)


@api.route('/profile-valentine')
@api.doc(
    description="Runs Valentine profiling between the table in the given asset and all other ingested tables, used for finding columns that are related.")
@api.doc(params={
    'asset_id': {'description': 'The id of the asset to get the table from', 'in': 'query', 'type': 'string', 'required': 'true'}
})
class ProfileValentine(Resource):
    @api.response(202, 'Success', TaskIdModel)
    @api.response(400, 'Missing asset id query parameter')
    @api.response(403, 'Table in asset has not been ingested yet')
    @api.response(404, 'Table or asset does not exist')
    def get(self):
        asset_id = request.args.get('asset_id')

        if not asset_id:
            return Response("Missing asset id query parameter", status=400)

        table_path = search.io_tools.get_table_path_from_asset_id(asset_id)
        if not table_path:
            return Response("Asset or table within does not exist", status=404)

        if not db.table_exists(table_path):
            return Response("Table in asset has not been ingested yet", status=403)

        task = profile_valentine_star.delay(table_path)
        db.save_celery_task(task.id, task.as_tuple())

        return Response(json.dumps({"task_id": task.id, "type": "Valentine Profiling"}), mimetype='application/json',
                        status=202)


@api.route('/add-table')
@api.doc(description="Initiates ingestion and profiling for the table in the given asset.")
@api.doc(params={
    'asset_id': {'description': 'The id of the asset to get the table from', 'in': 'query', 'type': 'string', 'required': 'true'}
})
class AddTable(Resource):
    @api.response(400, 'Missing asset id query parameter')
    @api.response(204, 'Table in asset was already processed')
    @api.response(404, 'Table or asset does not exist')
    def get(self):
        asset_id = request.args.get('asset_id')

        if not asset_id:
            return Response("Missing asset id query parameter", status=400)

        table_path = search.io_tools.get_table_path_from_asset_id(asset_id)
        if not table_path:
            return Response("Table or asset does not exist", status=404)

        if db.get_table(table_path):
            return Response("Table in asset was already processed", status=204)

        task = (add_table.si(table_path) | profile_valentine_star.si(table_path)).apply_async()
        db.save_celery_task(task.id, task.as_tuple())

        return Response(json.dumps({"task_id": task.id, "type": "Single Ingestion"}), mimetype='application/json',
                        status=202)


@api.route('/get-table-csv')
@api.doc(description="Gets a part of the table at the given path as CSV.")
@api.doc(params={
    'asset_id': {'description': 'The id of the asset to get the table from', 'in': 'query', 'type': 'string', 'required': 'true'},
    'rows': {'description': 'Number of rows to get from the top', 'in': 'query', 'type': 'string', 'required': 'true'}
})
class GetTableCSV(Resource):
    @api.response(400, 'Missing asset id or rows query parameters')
    @api.response(404, 'Table in asset does not exist')
    def get(self):
        asset_id = request.args.get('asset_id')
        rows = request.args.get('rows', type=int)
        if not asset_id or not rows:
            return Response('Missing asset id or rows query parameters', status=400)

        table_path = search.io_tools.get_table_path_from_asset_id(asset_id)
        if not table_path:
            return Response("Table in asset does not exist", status=404)

        return search.io_tools.get_ddf(table_path).head(rows).to_csv()


RelatedTableModel = api.model("RelatedTable", {
    'links': fields.List(fields.String),
    'explanation': fields.String
})


@api.route('/get-related')
@api.doc(description="Get all the assets on the path connecting the source and the target tables.")
@api.doc(params={
    'source_asset_id': {'description': 'The id of the asset to get the table from as source', 'in': 'query', 'type': 'string', 'required': 'true'},
    'target_asset_ids': {'description': 'The id of the asset to get the table from as target', 'in': 'query', 'type': 'array', 'items': {'type': 'string'}, 'required': 'true'},
})
class GetRelatedNodes(Resource):
    @api.response(200, 'Success',
                  api.model("RelatedTables", {"RelatedTables": fields.List(fields.Nested(RelatedTableModel))}))
    @api.response(400, 'Missing asset ids query parameters')
    @api.response(403, 'Source asset id is among target asset ids')
    @api.response(404, 'Table in asset does not exist')
    def get(self):
        source_asset_id = request.args.get("source_asset_id")

        # Normally we can use Flask's 'getlist', but Flask RestX does not correctly send the query params:
        # - What it should do is repeat the query param multiple times with different values
        # - Instead, it puts it as one query param and separates the values by commas...
        target_asset_ids_string = request.args.get("target_asset_ids")

        if not source_asset_id or not target_asset_ids_string:
            return Response("Please provide both a source asset id and a target asset id as query parameters",
                            status=400)

        target_asset_ids = target_asset_ids_string.split(',')

        if source_asset_id in target_asset_ids:
            return Response("Source asset id should not be in target asset ids", status=403)

        source_nodes = discovery.node_helper.get_nodes_path_contains(source_asset_id)
        if len(source_nodes) == 0:
            return Response("Table or asset does not exist", status=404)

        related_tables = []

        for source in source_nodes:
            print(f"From asset id: {source}")
            from_table = search.redis_tools.get_table(source)

            for asset_id in target_asset_ids:
                logging.info(f"Processing {asset_id}")
                target_nodes = discovery.node_helper.get_nodes_path_contains(asset_id)

                if len(target_nodes) == 0:
                    logging.warning(f"Given asset '{asset_id}' does not exist")
                    continue

                for target in target_nodes:
                    to_table = search.redis_tools.get_table(target)
                    if to_table is None:
                        continue
                    related_tables += get_related_between_two_tables(from_table, to_table)

        return Response(json.dumps({"RelatedTables": related_tables}), mimetype='application/json', status=200)


# Apparently we need to make models for every nested field...
# See: https://github.com/noirbizarre/flask-restplus/issues/292 (bug still exists in flask_restx)
MatchModel = api.model("Match", {
    'PK': fields.Nested(api.model("Relation", {"from_id": fields.String, "to_id": fields.String})),
    'RELATED': fields.Nested(api.model("Profiles", {"coma": fields.Float})),
    'explanation': fields.String
})
JoinableTableModel = api.model("JoinableTable", {
    'matches': fields.List(fields.Nested(MatchModel)),
    'table_name': fields.String
})


@api.route('/get-joinable')
@api.doc(description="Gets all assets that are joinable with the given source asset.")
@api.doc(params={
    'asset_id': {'description': 'The id of the asset to get the table from', 'in': 'query', 'type': 'string', 'required': 'true'}
})
class GetJoinable(Resource):
    @api.response(200, 'Success', api.model("JoinableTables", {
        "JoinableTables": fields.List(fields.Nested(JoinableTableModel))}))  # TODO: specify return model
    @api.response(400, 'Missing asset id query parameter')
    @api.response(404, 'Table or table does not exist')
    def get(self):
        args = request.args
        asset_id = args.get("asset_id")
        if asset_id is None:
            return Response("Please provide an asset id as query parameter", status=400)

        nodes = discovery.node_helper.get_nodes_path_contains(asset_id)
        if len(nodes) == 0:
            return Response("Table or asset does not exist", status=404)

        joinable_tables = []
        for node in nodes:
            logging.info(f"Asset id: {node}")
            table = search.redis_tools.get_table(node)
            if table is None:
                continue
            joinable_tables.extend(discovery.queries.get_joinable(table))

        return Response(json.dumps({"JoinableTables": joinable_tables}),
                        mimetype='application/json', status=200)


DEFAULT_PORT = 8080

if __name__ == "__main__":
    if app.debug:
        app.run(host='0.0.0.0', port=DEFAULT_PORT)
    else:
        from waitress import serve
        serve(app, host="0.0.0.0", port=DEFAULT_PORT)

