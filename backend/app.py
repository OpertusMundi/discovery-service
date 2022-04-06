#!/usr/bin/env python

import json
import logging
import os

import requests
from flask import Response
from flask import request, make_response, Flask

from celery import group, chord

# Import own modules
from backend import app, celery
from backend import discovery
from backend import profiling
from backend import search
from backend.discovery import relation_types
from backend.profiling.valentine import process_match
from backend.utility.display import log_format
from backend.utility.parsing import parse_binder_results
from backend.utility.celery_tasks import add_table, profile_valentine_all

# Display/logging settings
logging.basicConfig(format=log_format, level=logging.INFO)


@app.route('/')
def index():
    return '"I live... Again"'


@app.route('/ingest-data/<path:bucket>')
def ingest_data(bucket):
    header = []
    for table_path in search.io_tools.get_tables(bucket):
        if not search.mongo_tools.get_table(table_path):
            header.append(add_table.s(bucket, table_path))
        else:
            logging.info(f"Table {table_path} was already processed!")

    chord(header)(profile_valentine_all.si(bucket))
    return Response('Success', 200)


@app.route('/purge')
def purge():
    # search.es_tools.init_indices(purge=True)
    search.mongo_tools.purge()
    discovery.crud.delete_all_nodes()
    return Response('Success', 200)


# TODO: metanome runs for all tables at once, consider running it only for specific tables
@app.route('/profile-metanome')
def profile_metanome():
    # Run binder on metanome and obtain constraints
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
        print(constraint)
        relation = discovery.crud.create_relation(constraint[0], constraint[1],
                                                  discovery.relation_types.FOREIGN_KEY_METANOME)

    return Response('Success', 200)


# Expected JSON format:
# {
#     'table1_path': {PATH}, 
#     'table2_path': {PATH}
# }
@app.route('/profile-valentine', methods=['POST'])
def profile_valentine():
    table1_path = request.json['table1_path']
    table2_path = request.json['table2_path']
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


# Expected JSON format:
# {
#     'table_path' : {PATH}
# }
@app.route('/add-table', methods=['POST'])
def add_table_route():
    table_path = request.json['table_path']
    add_table(table_path)
    return f"Starting to ingest newly added table {table_path}"


# Expected JSON format:
# {
#     'table_path' : {PATH},
#     'rows'       : {NUMBER >= 0}
# }
@app.route('/get-table-csv', methods=['POST'])
def get_table_csv():
    table_path = request.json['table_path']
    rows = request.json['rows']
    return search.io_tools.get_ddf(table_path).head(rows).to_csv()


# TODO: needs API revision
@app.route('/get-related/<path:node_id>', methods=['GET'])
def get_related_nodes(node_id):
    return Response(json.dumps(discovery.queries.get_related_nodes(node_id)), mimetype='application/json', status=200)


# TODO: needs API revision
@app.route('/get-joinable/<path:table_name>', methods=['GET'])
def get_joinable(table_name):
    return Response(json.dumps(discovery.queries.get_joinable(table_name)), mimetype='application/json', status=200)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=443)
