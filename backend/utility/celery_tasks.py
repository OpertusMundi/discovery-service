import itertools
import logging
import os
import uuid

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
    df = search.io_tools.get_df(bucket, table_path)
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
    for table_path_1, table_path_2 in itertools.combinations(all_tables, r=2):
        profile_valentine_pair(bucket, table_path_1, table_path_2)


@celery.task
def profile_valentine_star(bucket, table_path):
    all_tables = io_tools.get_tables(bucket)
    for other_table_path in all_tables:
        if table_path != other_table_path:
            profile_valentine_pair(bucket, table_path, other_table_path)


@celery.task
def profile_valentine_pair(bucket, table_path_1, table_path_2):
    logging.info(f'Valentining files: {table_path_1}, {table_path_2}')
    rows_to_use = int(os.environ['VALENTINE_ROWS_TO_USE'])
    df1 = search.io_tools.get_df(bucket, table_path_1, rows=rows_to_use)
    df2 = search.io_tools.get_df(bucket, table_path_2, rows=rows_to_use)
    matches = match(df1, df2)
    process_match(table_path_1, table_path_2, matches)



