#!/usr/bin/env python

import json
import logging
import os

import pandas as pd
import requests
from celery import Celery
from flask import Flask
from flask import Response
from flask import request

from .. import discovery
# Import own modules
from .. import profiling
from .. import search
from ..utility.display import pd_display_context_settings, log_format
from ..utility.parsing import parse_binder_results

# Display/logging settings
logging.basicConfig(format=log_format, level=logging.INFO)

# Flask configuration
app = Flask(__name__)
app.debug = True
app.secret_key = os.urandom(24)

celery = Celery(app.name)
celery.conf.update(broker_url=f"amqp://{os.environ['RABBITMQ_DEFAULT_USER']}:"
                              f"{os.environ['RABBITMQ_DEFAULT_PASS']}@"
                              f"{os.environ['RABBITMQ_HOST']}:"
                              f"{os.environ['RABBITMQ_PORT']}/")


@app.route('/')
def index():
    return '"I live... Again"'


@app.route('/start')
def start():
    # search.es_tools.init_indices()
    tables = []
    for table_path in search.io_tools.get_tables():
        if not search.mongo_tools.get_table(table_path):
            add_table(table_path)
            tables.append(table_path)
        else:
            logging.info(f"Table {table_path} was already processed!")

    return f"Successfully ingested tables: {', '.join(tables)}"


@app.route('/purge')
def purge():
    # search.es_tools.init_indices(purge=True)
    search.mongo_tools.purge()
    discovery.crud.delete_all_nodes()
    return "Purged successfully!"


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

    logging.info("Purging old metanome constraints...")
    discovery.crud.delete_relations_by_name(discovery.relation_types.FOREIGN_KEY_METANOME)

    logging.info("Adding new metanome constraints to neo4j...")
    for constraint in constraints:
        relation = discovery.crud.create_relation(constraint[0], constraint[1],
                                                  discovery.relation_types.FOREIGN_KEY_METANOME)

    return "Successfully profiled all tables!"


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

    # TODO: can potentially move this into the valentine helper module
    threshold = float(os.environ['VALENTINE_THRESHOLD'])
    node_ids_t1 = search.mongo_tools.get_node_ids(table1_path)
    node_ids_t2 = search.mongo_tools.get_node_ids(table2_path)
    for ((_, col_from), (_, col_to)), similarity in matches.items():
        if similarity > threshold:
            discovery.crud.create_relation(node_ids_t1[col_from], node_ids_t2[col_to], discovery.relation_types.MATCH)
            discovery.crud.set_relation_properties(node_ids_t1[col_from], node_ids_t2[col_to],
                                                   discovery.relation_types.MATCH, coma=similarity)

    return str(f"Successfully profiled {table1_path} and {table2_path} with threshold {threshold}!")


# Expected JSON format:
# {
#     'table_path' : {PATH}
# }
@app.route('/profile-sherlock', methods=['POST'])
def profile_sherlock():
    table_path = request.json['table_path']

    df = search.io_tools.get_ddf(table_path).head(10000)
    res = profiling.sherlock.predict(df)
    node_ids = search.mongo_tools.get_node_ids(table_path)

    for col, pred in zip(list(df.columns), list(res)):
        discovery.crud.set_node_properties(node_ids[col], sherlock=pred)

    return f"Successfully profiled {table_path}!"


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


# TODO: break this up, method is too large
def add_table(table_name):
    ddf = search.io_tools.get_ddf(table_name)
    logging.info(f"Processing table '{table_name}'")

    with pd.option_context(*pd_display_context_settings):
        logging.info("- Table sample: \n" + str(ddf.head(10)))

    logging.info("- Getting profile")
    profile = profiling.pandas.get_profile(ddf, table_name)

    with pd.option_context(*pd_display_context_settings):
        logging.info("- Profile: \n" + str(profile))

    logging.info(f"- Adding whole table metadata to neo4j")
    nodes = {}
    for col in ddf.columns:
        node = discovery.crud.create_node(col, table_name)
        node_id = node[0]['id']
        nodes[col] = node_id

        column_profile = profiling.pandas.convert_to_python_types(profile[col].to_dict())
        discovery.crud.set_node_properties(node_id, **column_profile)
    discovery.crud.create_subsumption_relation(table_name)

    logging.info(f"- Adding ingestion record to mongodb")
    search.mongo_tools.add_table(table_name, len(ddf.columns), nodes)

    return f"Successfully ingested table {table_name}"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=443)
