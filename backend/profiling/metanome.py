import logging
import os

import requests

from backend.discovery import relation_types
from backend.discovery.crud import create_relation, set_relation_properties
from backend.utility.parsing import parse_binder_results


def profile_metanome() -> None:
    """
    Runs BINDER on Metanome and parses its results, creating new FK relations in Neo4J between the relevant nodes.
    """
    logging.info("Connecting to Metanome and waiting for Binder results...")
    address = os.environ["METANOME_API_ADDRESS"]
    binder_res = requests.get(f'http://{address}/run_binder')

    if binder_res.status_code == 500:
        logging.error(f"Waiting time for Binder results exceeded timeout")

    if binder_res.status_code >= 400:
        logging.error(f"Could not reach Metanome! Status: {binder_res.status_code}")

    binder_data = binder_res.content.decode("utf-8")
    if binder_data:
        logging.info("Parsing obtained results from metanome...")
        constraints = parse_binder_results(binder_data)

        logging.info("Adding metanome constraints to neo4j...")
        for constraint in constraints:
            create_relation(constraint[0], constraint[1], relation_types.FOREIGN_KEY_METANOME)
            set_relation_properties(constraint[0], constraint[1], relation_types.FOREIGN_KEY_METANOME,
                                    from_id=constraint[0], to_id=constraint[1])
    else:
        logging.error("Binder results were empty, ")
