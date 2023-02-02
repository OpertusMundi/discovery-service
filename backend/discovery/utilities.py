import logging
from typing import List, Dict, Union

from backend.utility.display import log_format

logging.basicConfig(format=log_format, level=logging.ERROR)


def process_relation(base_table_path: str, base_table_name: str, result: List[Dict[str, str]]) -> List[Dict[str, Union[Dict[str, str], str]]]:
    tables = []
    assets = []
    for relation in result:
        table = {}
        # Get connected node to the sibling. A relation is only between 2 nodes, so filter outputs one result
        related_node = list(filter(lambda x: x.get('id') is not None, relation.nodes))[0]
        table['table_path'] = related_node.get('source_path')
        table['table_name'] = related_node.get('source_name')  # Needed for display
        if table['table_path'] in assets:
            logging.info(f"IN ASSETS: {table['table_path']}")
            continue

        assets.append(table['table_path'])
        logging.info(f"TABLE PATH: {table['table_path']}")

        explanation = f"Table {base_table_path} is joinable with table {related_node.get('source_name')}"

        if "to_id" in relation:
            table["PK"] = {'from_id': relation['from_id'], 'to_id': relation['to_id']}
            explanation = f"{explanation} via the primary-key foreign-key constraint: PK - {relation['from_id']} and " \
                          f"FK - {relation['to_id']}"
        if "coma" in relation:
            table["RELATED"] = {'coma': relation['coma']}
            explanation = f"{explanation} with a confidence threshold of {relation['coma']} (1.0 being the best score)"
        else:
            table["RELATED"] = {}

        table['explanation'] = explanation
        tables.append(table)
    return tables


def process_node(result: List[Dict[str, str]]) -> List[Dict[str, str]]:
    return list(map(lambda x: {'table': x.get('source_path'), 'column': x.get('name'), 'id': x.get('id')}, result))
