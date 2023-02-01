import itertools
import logging
import os
import logging

from celery.app.task import Task
from backend import celery
from .. import search, profiling, discovery
from ..profiling.valentine import match, process_match
from ..profiling.ind_finder import find_inclusion_dependencies
from ..discovery.queries import delete_spurious_connections
from ..search import io_tools
from ..search import redis_tools as db


logger = logging.getLogger(__name__)


# The default class to use for logging exceptions properly and not hang
class LoggingTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        kwargs = {}
        if logger.isEnabledFor(logging.DEBUG):
            kwargs['exc_info'] = exc
        logger.error('Task %s failed to execute', task_id, **kwargs)


@celery.task
def ingest_all_new_tables():
    paths = search.io_tools.get_tables()
    if not paths:
        logging.warn(
            "No tables to process, make sure there is data present on the data volume...")
    else:
        to_process = []
        for table_path in paths:
            if not db.table_exists(table_path):
                logging.info(f"Found new table to process: {table_path}")
                to_process.append(table_path)

        if to_process:
            logging.info(f"Processing new table: {to_process}")
            for table_path in to_process:
                add_table(table_path)
            for table_path in to_process:
                profile_valentine_star(table_path)
                find_inds_star(table_path)

            logging.info("Cleaning up...")
            delete_spurious_connections()
        else:
            logging.info("No new tables to process")


@celery.task
def add_table(table_path: str):
    """
    Adds a table at the given table path to Daisy's databases.
    """
    table_name = table_path.split('/')[-1]
    logging.info(f"- Parsing table at {table_path} into DataFrame")
    df = search.io_tools.get_df(table_path)
    # Split the dataframe into a new dataframe for each column
    logging.info(f"- Adding whole table metadata to neo4j for {table_path}")
    nodes = {}
    for col in df.columns:
        node = discovery.crud.create_node(table_name, table_path, col)
        node_id = node[0]['id']
        nodes[col] = node_id

        discovery.crud.set_node_properties(
            node_id, **profiling.pandas.get_profile_column(df[col]))

    discovery.crud.create_subsumption_relation(table_path)

    logging.info(f"- Adding ingestion record to db")

    db.add_table(table_name, table_path, len(df.columns), nodes)


@celery.task
def profile_valentine_all():
    """
    Profiles all tables against each other.
    """
    all_tables = io_tools.get_tables()
    for table_path_1, table_path_2 in itertools.combinations(all_tables, r=2):
        profile_valentine_pair(table_path_1, table_path_2)


@celery.task
def profile_valentine_star(table_path: str):
    """
    Profiles all other tables against the table at the given path.
    """
    all_tables = db.list_tables()
    for other in all_tables:
        if table_path != other["path"]:
            profile_valentine_pair(table_path, other["path"])


@celery.task
def profile_valentine_pair(table_path_1: str, table_path_2: str):
    """
    Profiles the two tables at the given paths against each other.
    """
    logging.info(f'Valentining files: {table_path_1}, {table_path_2}')
    rows_to_use = int(os.environ['VALENTINE_ROWS_TO_USE'])
    df1 = search.io_tools.get_df(table_path_1, rows=rows_to_use)
    df2 = search.io_tools.get_df(table_path_2, rows=rows_to_use)
    matches = match(df1, df2)
    process_match(table_path_1, table_path_2, matches)


@celery.task
def find_inds_pair(table_path_1: str, table_path_2: str):
    logging.info(f'Finding INDs between: {table_path_1}, {table_path_2}')
    find_inclusion_dependencies([table_path_1, table_path_2])


@celery.task
def find_inds_star(table_path: str):
    all_tables = db.list_tables()
    for other in all_tables:
        if table_path != other["path"]:
            find_inds_pair(table_path, other["path"])


@celery.task
def find_inds_all():
    all_tables = io_tools.get_tables()
    for table_path_1, table_path_2 in itertools.combinations(all_tables, r=2):
        find_inds_pair(table_path_1, table_path_2)
