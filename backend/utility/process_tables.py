import logging
import os
import uuid
from io import StringIO

import pandas as pd

from .. import search, profiling, discovery
from backend import celery
from ..clients import minio


@celery.task
def add_table(table_name):
    csv_string = minio.get_client().get_object(os.environ["MINIO_DEFAULT_BUCKET"], table_name).data.decode("utf-8")
    df = pd.read_csv(StringIO(csv_string), header=0, engine="python", encoding="utf8", quotechar='"', escapechar='\\')

    # Split the dataframe into a new dataframe for each column
    logging.info(f"- Adding whole table metadata to neo4j")
    nodes = {}
    for col in df.columns:
        node = discovery.crud.create_node(col, table_name)
        node_id = node[0]['id']
        nodes[col] = node_id

        discovery.crud.set_node_properties(node_id, **profiling.pandas.get_profile_column(df[col]))

    discovery.crud.create_subsumption_relation(table_name)

    logging.info(f"- Adding ingestion record to mongodb")
    search.mongo_tools.add_table(table_name, len(df.columns), nodes)

