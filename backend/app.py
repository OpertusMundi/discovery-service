#!/usr/bin/env python

import json
import logging
import os

import requests
from celery import chord, group
from flask import Response
from flask import request
from flask_restx import Resource, fields

# Import own modules
from backend import app
from backend import api
from backend import discovery
from backend import profiling
from backend import search
from backend import celery as celery_app
from backend.discovery import relation_types
from backend.discovery.queries import delete_spurious_connections, get_related_between_two_tables
from backend.profiling.valentine import process_match
from backend.profiling.metanome import profile_metanome
from backend.utility.celery_tasks import add_table, profile_valentine_all
from backend.utility.display import log_format

# Display/logging settings
logging.basicConfig(format=log_format, level=logging.INFO)


@api.route('/ingest-data')
@api.doc(description="Ingest all the data located at the given bucket.")
@api.doc(params={
    'bucket':  {'description': 'Path to the S3 bucket with data', 'in': 'query', 'type': 'string', 'required': 'true'},
})
class IngestData(Resource):
    @api.response(202, 'Success', api.model('IngestDataResponse', {
        'task_id': fields.String, 
    }))
    @api.response(404, 'Bucket does not exist')
    @api.response(400, 'Bucket is empty')
    def get(self):
        bucket = request.args.get("bucket")

        if not search.io_tools.bucket_exists(bucket):
            return Response("Bucket does not exist", 404)
        paths = search.io_tools.get_tables(bucket)
        if len(paths) == 0:
            return Response("Bucket is empty", 400)
        header = []
        for table_path in paths:
            if not search.mongo_tools.get_table(table_path):
                header.append(add_table.s(bucket, table_path))
            else:
                logging.info(f"Table {table_path} was already processed!")
        
        task_group = group(*header)
        profiling_chord = chord(task_group)(profile_valentine_all.si(bucket))
        profiling_chord.parent.save()
        search.mongo_tools.store_celery_task_id(profiling_chord.parent.id, profiling_chord.id)

        return Response(json.dumps({"task_id": profiling_chord.parent.id}), mimetype='application/json', status=202)


TaskStatusModel = api.model("TaskStatus", {
    'task_name': fields.String, 
    'task_id': fields.String, 
    'task_status': fields.String
})
@api.route('/task-status')
@api.doc(description="Checks the status of a task.")
@api.doc(params={
    'task_id':  {'description': 'ID of task to check', 'in': 'query', 'type': 'string', 'required': 'true'},
})
# Based on solution in: https://github.com/celery/celery/issues/4516
class TaskStatus(Resource):
    @api.response(200, 'Success', api.model('TaskStatusResponse', {
        'profiling_callback_status': fields.Nested(TaskStatusModel), 
        'ingestion_tasks': fields.List(fields.Nested(TaskStatusModel))
        }))
    @api.response(404, 'No such task')
    def get(self):
        parent_id = request.args.get("task_id")
        parent = celery_app.GroupResult.restore(parent_id)
        task_id = search.mongo_tools.get_celery_task_id(parent_id)
        if not task_id:
            return Response("Task does not exist", 404)
        task = celery_app.AsyncResult(task_id, parent=parent)
        return Response(json.dumps({
            "profiling_callback_status": {'task_name': f'{task.name} ({str(task.args)})', 'task_id': task.id, 'task_status': task.status},
            "ingestion_task_statuses": [{'task_name': f'{child.name} ({str(child.args)})', 'task_id': child.id, 'task_status': child.status} for child in task.parent.children]
            }), mimetype='application/json', status=200)
        


@api.route('/purge')
@api.doc(description="Purges all of the databases.")
class Purge(Resource):
    @api.response(200, 'Success')
    def get(self):
        search.mongo_tools.purge()
        discovery.crud.delete_all_nodes()
        return Response('Success', status=200)


# TODO: metanome runs for all tables at once, consider running it only for specific tables - on hold for now
# TODO: Refactor and add error checks (move the logic to another module)
# TODO: Implement the algorithm for PK-FK and remove metanome
@api.route('/profile-metanome')
@api.doc(description="Runs Metanome profiling for all tables, which is used to obtain KFK relations between the tables.")
@api.doc(params={
    'bucket':  {'description': 'Path to the S3 bucket with data', 'in': 'query', 'type': 'string', 'required': 'true', 'default': 'data'},
})
class ProfileMetanome(Resource):
    @api.response(200, 'Success')
    @api.response(404, 'Bucket does not exist')
    @api.response(500, 'Cannot connect to Metanome')
    def get(self):
        bucket = request.args.get('bucket')
        if not search.io_tools.bucket_exists(bucket):
            return Response("Bucket does not exist", 404)
        try:
            profile_metanome(os.environ["METANOME_API_ADDRESS"], bucket)
            return Response('Success', status=200)
        except ConnectionError:
            return Response('Cannot connect to Metanome', status=500)



@api.route('/filter-connections')
@api.doc(description="Filters spurious connections. This step is required after the ingestion phase.")
class FilterConnections(Resource):
    # NOTE: We can only have a single response per code, see: https://github.com/python-restx/flask-restx/issues/274
    @api.response(200, 'Success', api.model('DeletedRelations', {
        'deleted_relations': fields.List(fields.String)
        }))
    def get(self):
        deleted_relations = delete_spurious_connections()
        return Response(json.dumps({"deleted_relations": deleted_relations}), mimetype='application/json', status=200)


# TODO: Refactor it - make it ready for adding new tables on the fly
@api.route('/profile-valentine')
@api.doc(description="Runs Valentine profiling between the given tables, used for finding columns that are related.")
class ProfileValentine(Resource):
    @api.expect(api.model('ProfileValentine', {
        'table_path_1': fields.String(description='Path to the first table', required=True), 
        'table_path_2': fields.String(description='Path to the second table', required=True)
    }))
    def post(self):
        table1_path = api.payload['table1_path']
        table2_path = api.payload['table2_path']
        df1 = search.io_tools.get_ddf(table1_path).head(10000)
        df2 = search.io_tools.get_ddf(table2_path).head(10000)
        matches = profiling.valentine.match(df1, df2)

        process_match(table1_path, table2_path, matches)

        threshold = float(os.environ['VALENTINE_THRESHOLD'])
        # node_ids_t1 = search.mongo_tools.get_node_ids(table1_path)
        # node_ids_t2 = search.mongo_tools.get_node_ids(table2_path)
        # for ((_, col_from), (_, col_to)), similarity in matches.items():
        #     if similarity > threshold:
        #         discovery.crud.create_relation(node_ids_t1[col_from], node_ids_t2[col_to], relation_types.MATCH)
        #         discovery.crud.set_relation_properties(node_ids_t1[col_from], node_ids_t2[col_to],
        #                                                relation_types.MATCH, coma=similarity)

        return str(f"Successfully profiled {table1_path} and {table2_path} with threshold {threshold}!")


# TODO: Add the profiling methods to the new tables
@api.route('/add-table')
@api.doc(description="Initiates ingestion for the table at the given S3 path.")
class AddTable(Resource):
    @api.expect(api.model('AddTable', {
        'table_path': fields.String(description='Path to the table', required=True), 
    }))
    def post(self):
        table_path = api.payload['table_path']
        add_table(table_path)
        return f"Starting to ingest newly added table {table_path}"


# TODO: Refactor - make it consistent with the rest of the endpoints (make it a get instead of post)
@api.route('/get-table-csv')
@api.doc(description="Gets a part of the table at the given path as CSV.")
class GetTableCSV(Resource):
    @api.expect(api.model('GetTableCSV', {
        'bucket_path': fields.String(description='Name of the bucket containing the table', required=True), 
        'table_path': fields.String(description='Path to the table', required=True), 
        'rows': fields.Integer(description='Number of rows to get', required=True, min=0)
    }))
    def post(self):
        bucket_path = api.payload['bucket_path']
        table_path = api.payload['table_path']
        rows = api.payload['rows']
        return search.io_tools.get_ddf(bucket_path, table_path).head(rows).to_csv()


@api.route('/get-related')
@api.doc(description="Get all the assets on the path connecting the source and the target tables.")
@api.doc(params={
    'source_table':  {'description': 'Table name', 'in': 'query', 'type': 'string', 'required': 'true'},
    'target_table':  {'description': 'Table name', 'in': 'query', 'type': 'string', 'required': 'true'}
})
class GetRelatedNodes(Resource):
    def get(self):
        args = request.args
        from_table = args.get("source_table")
        to_table = args.get("target_table")

        if not from_table or not to_table:
            return Response("Please provide the start and the end of the path", status=400)

        node = discovery.queries.get_node_by_prop(source_name=from_table)
        if len(node) == 0:
            return Response("Table does not exist", status=404)

        node = discovery.queries.get_node_by_prop(source_name=to_table)
        if len(node) == 0:
            return Response("Table does not exist", status=404)

        paths = get_related_between_two_tables(from_table, to_table)
        return Response(json.dumps(paths), mimetype='application/json', status=200)


@api.route('/get-joinable')
@api.doc(description="Gets all assets that are joinable with the given source table.")
@api.doc(params={
    'source_table':  {'description': 'Table name', 'in': 'query', 'type': 'string', 'required': 'true'}
})
class GetJoinable(Resource):
    def get(self):
        args = request.args
        table_name = args.get("source_table")
        if table_name is None:
            return Response("Please provide a table name", status=400)

        node = discovery.queries.get_node_by_prop(source_name=table_name)
        if len(node) == 0:
            return Response("Table does not exist", status=404)

        return Response(json.dumps(discovery.queries.get_joinable(table_name)), mimetype='application/json', status=200)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=443)
