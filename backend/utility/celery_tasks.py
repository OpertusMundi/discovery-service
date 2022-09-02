import itertools
import logging
import os
import uuid
from io import StringIO

import pandas as pd

from .. import search, profiling, discovery
from backend import celery
from ..clients import minio
from ..clients.minio import minio_client
from ..profiling.valentine import match, process_match
from ..search import io_tools


# return value to know if it succeed or not
@celery.task
def add_table(bucket, table_path):
    table_name = table_path.split('/')[-1]
    csv_string = minio.minio_client.get_object(bucket, table_path).data.decode("utf-8")
    df = pd.read_csv(StringIO(csv_string), header=0, engine="python", encoding="utf8", quotechar='"', escapechar='\\')

    # Split the dataframe into a new dataframe for each column
    logging.info(f"- Adding whole table metadata to neo4j")
    nodes = {}
    for col in df.columns:
        node = discovery.crud.create_node(table_name, table_path, col)
        node_id = node[0]['id']
        nodes[col] = node_id

        discovery.crud.set_node_properties(node_id, **profiling.pandas.get_profile_column(df[col]))

    discovery.crud.create_subsumption_relation(table_path)

    logging.info(f"- Adding ingestion record to mongodb")

    search.mongo_tools.add_table(table_name, table_path, bucket, len(df.columns), nodes)


@celery.task
def profile_valentine_all(bucket):
    all_tables = io_tools.get_tables(bucket)
    for table_pair in itertools.combinations(all_tables, r=2):
        (tab1_path, tab2_path) = table_pair
        csv_string = minio_client.get_object(bucket, tab1_path).data.decode("utf-8")
        df_tab1 = pd.read_csv(StringIO(csv_string), header=0, engine="python", encoding="utf8", quotechar='"',
                         escapechar='\\', nrows=1000)
        logging.warning(f'Valentining files: {table_pair}')
        csv_string = minio_client.get_object(bucket, tab2_path).data.decode("utf-8")
        df_tab2 = pd.read_csv(StringIO(csv_string), header=0, engine="python", encoding="utf8", quotechar='"',
                              escapechar='\\', nrows=1000)
        matches = match(df_tab1, df_tab2)
        process_match(tab1_path, tab2_path, matches)

