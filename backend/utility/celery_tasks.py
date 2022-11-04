import itertools
import logging
import os

from backend import celery
from .. import search, profiling, discovery
from ..profiling.valentine import match, process_match
from ..profiling.metanome import profile_metanome
from ..discovery.queries import delete_spurious_connections
from ..search import io_tools
from ..search import redis_tools as db


@celery.task
def ingest_all_new_tables():
    bucket = 'data'
    paths = search.io_tools.get_tables(bucket)
    if not paths:
        logging.warn("No tables to process, make sure there is data present on the data volume...")
    else:
        to_process = []
        for table_path in paths:
            if not db.table_exists(table_path):
                logging.info(f"Found new table to process: {table_path}")
                to_process.append(table_path)

        if to_process:
            logging.info(f"Processing new table: {to_process}")
            for table_path in to_process:
                add_table(bucket, table_path)
                profile_valentine_star(bucket, table_path)
            logging.info("Starting Metanome FK profile")
            profile_metanome(bucket)
            logging.info("Cleaning up...")
            delete_spurious_connections()
        else:
            logging.info("No new tables to process")


# return value to know if it succeeded or not
@celery.task
def add_table(bucket: str, table_path: str):
    """
    Adds a table in the given bucket and at the given table path to Daisy's databases.
    """
    table_name = table_path.split('/')[-1]
    df = search.io_tools.get_df(bucket, table_path)
    # Split the dataframe into a new dataframe for each column
    logging.info(f"- Adding whole table metadata to neo4j for {table_path}")
    nodes = {}
    for col in df.columns:
        node = discovery.crud.create_node(table_name, table_path, col)
        node_id = node[0]['id']
        nodes[col] = node_id

        discovery.crud.set_node_properties(node_id, **profiling.pandas.get_profile_column(df[col]))

    discovery.crud.create_subsumption_relation(table_path)

    logging.info(f"- Adding ingestion record to db")

    db.add_table(table_name, table_path, bucket, len(df.columns), nodes)


@celery.task
def profile_valentine_all(bucket: str):
    """
    Profiles all tables in the given bucket against each other.
    """
    all_tables = io_tools.get_tables(bucket)
    for table_path_1, table_path_2 in itertools.combinations(all_tables, r=2):
        profile_valentine_pair(bucket, table_path_1, table_path_2)


@celery.task
def profile_valentine_star(bucket: str, table_path: str):
    """
    Profiles all tables in the given bucket against the given table.
    """
    all_tables = db.list_tables(bucket=bucket)
    for other in all_tables:
        if table_path != other["path"]:
            profile_valentine_pair(bucket, table_path, other["path"])


@celery.task
def profile_valentine_pair(bucket: str, table_path_1: str, table_path_2: str):
    """
    Profiles the two given tables in the given bucket against the given table.
    """
    logging.info(f'Valentining files: {table_path_1}, {table_path_2}')
    rows_to_use = int(os.environ['VALENTINE_ROWS_TO_USE'])
    df1 = search.io_tools.get_df(bucket, table_path_1, rows=rows_to_use)
    df2 = search.io_tools.get_df(bucket, table_path_2, rows=rows_to_use)
    matches = match(df1, df2)
    process_match(table_path_1, table_path_2, matches)
