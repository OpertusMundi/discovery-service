#!/usr/bin/env python

import json
import logging
import os

import requests
from celery import chord
from flask import Response
from flask import request
from flask_restx import Resource, fields

# Import own modules
from backend import app
from backend import api
from backend import discovery
from backend import profiling
from backend import search
from backend.discovery import relation_types
from backend.discovery.queries import delete_spurious_connections, get_related_between_two_tables
from backend.profiling.valentine import process_match
from backend.utility.celery_tasks import add_table, profile_valentine_all
from backend.utility.display import log_format
from backend.utility.parsing import parse_binder_results

# Display/logging settings
logging.basicConfig(format=log_format, level=logging.INFO)

@api.route('/')
@api.doc(description="The base route, for testing whether the API is reachable.")
class Index(Resource):
    def get(self):
        return '"I live... Again"'


@api.route('/ingest-data/<path:bucket>')
@api.doc(description="Ingest all the data located at the given bucket.")
@api.doc(params={'bucket': {'description': 'Path to the S3 bucket with data', 'required': True}})
class IngestData(Resource):
    def get(self, bucket):
        header = []
        for table_path in search.io_tools.get_tables(bucket):
            if not search.mongo_tools.get_table(table_path):
                print(table_path)
                header.append(add_table.s(bucket, table_path))
            else:
                logging.info(f"Table {table_path} was already processed!")

        chord(header)(profile_valentine_all.si(bucket))
        return Response('Success', 200)


@api.route('/purge')
@api.doc(description="Purges all of the databases.")
class Purge(Resource):
    def get(self):
        # search.es_tools.init_indices(purge=True)
        search.mongo_tools.purge()
        discovery.crud.delete_all_nodes()
        return Response('Success', 200)


# TODO: metanome runs for all tables at once, consider running it only for specific tables
@api.route('/profile-metanome')
@api.doc(description="Runs Metanome profiling for all tables, which is used to obtain column relations between the tables.")
class ProfileMetanome(Resource):
    def get(self):
        logging.info("Attempting to connect to Metanome...")

        address = os.environ["METANOME_API_ADDRESS"]
        binder_res = requests.get(f'http://{address}/run_binder')

        if binder_res.status_code >= 400:
            raise ConnectionError(f"Could not reach Metanome! Status: {binder_res.status_code}")

        binder_data = binder_res.content.decode("utf-8")

        logging.info("Parsing obtained results from metanome...")
        constraints = parse_binder_results(binder_data)

        logging.info("Adding metanome constraints to neo4j...")
        for constraint in constraints:
            relation = discovery.crud.create_relation(constraint[0], constraint[1],
                                                      discovery.relation_types.FOREIGN_KEY_METANOME)
            discovery.crud.set_relation_properties(constraint[0], constraint[1], relation_types.FOREIGN_KEY_METANOME,
                                                   from_id=constraint[0], to_id=constraint[1])

        return Response('Success', 200)


@api.route('/filter-connections')
@api.doc(description="Filters spurious connections.")
class FilterConnections(Resource):
    def get(self):
        deleted_relations = delete_spurious_connections()
        if len(deleted_relations) == 0:
            return Response("No relationships affected", status=200)
        return Response(json.dumps(deleted_relations), mimetype='application/json', status=200)


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


@api.route('/get-table-csv')
@api.doc(description="Gets a part of the table at the given path as CSV.")
class GetTableCSV(Resource):
    @api.expect(api.model('GetTableCSV', {
        'table_path': fields.String(description='Path to the table', required=True), 
        'rows': fields.Integer(description='Number of rows to get', required=True, min=0)
    }))
    def post(self):
        table_path = api.payload['table_path']
        rows = api.payload['rows']
        return search.io_tools.get_ddf(table_path).head(rows).to_csv()


@api.route('/get-related')
@api.doc(description="Gets all columns between two tables that are related somehow according to the profiling data.")
@api.doc(params={
    'from_table':  {'description': 'Path to the first table', 'in': 'query', 'type': 'string', 'required': 'true'}, 
    'to_table':  {'description': 'Path to the second table', 'in': 'query', 'type': 'string', 'required': 'true'}
})
# NOTE: API is very inconsistent in how it passes arguments, perhaps we should unify this?
class GetRelatedNodes(Resource):
    def get(self):
        args = request.args
        from_table = args.get("from_table")
        to_table = args.get("to_table")

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
@api.doc(description="Gets all columns that are joinable on this table")
@api.doc(params={
    'from_table':  {'description': 'Path to the table', 'in': 'query', 'type': 'string', 'required': 'true'}
})
class GetJoinable(Resource):
    def get(self):
        args = request.args
        table_name = args.get("table_name")
        if table_name is None:
            return Response("Please provide a table name", status=400)

        node = discovery.queries.get_node_by_prop(source_name=table_name)
        if len(node) == 0:
            return Response("Table does not exist", status=404)

        return Response(json.dumps(discovery.queries.get_joinable(table_name)), mimetype='application/json', status=200)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=443)
