from os import listdir
from os.path import isfile, join

import pandas as pd
from neo4j import GraphDatabase

from . import node_helper
from . import edge_helper
from .utilities import process_relation, process_node


def get_nodes():
    return node_helper.get_all()


def get_node_by_prop(**kwargs):
    return node_helper.get_node(**kwargs)

def get_related_nodes(node_id):
    node = {'id': node_id}
    nodes = [node]
    related_nodes = []

    while len(nodes) > 0:
        result = node_helper.get_related_nodes(nodes.pop(0)['id'])
        processed_result = process_relation(result)

        if len(nodes) == 0:
            nodes = processed_result
        else:
            nodes_id = list(map(lambda z: z['id'], nodes))
            temp = list(filter(lambda x: x['id'] not in nodes_id, processed_result))
            nodes = nodes + temp

        if len(related_nodes) == 0:
            related_nodes = processed_result
        else:
            nodes_id = list(map(lambda z: z['id'], related_nodes))
            nodes = list(filter(lambda x: x['id'] not in nodes_id, nodes))
            temp = list(filter(lambda x: x['id'] not in nodes_id, processed_result))
            related_nodes = related_nodes + temp

    return related_nodes


def get_joinable(table_name):
    # Get all the nodes belonging to the given table
    nodes = node_helper.get_nodes_by_table_name(table_name)
    # Simplify the object (only keep the table path, column name and column id)
    siblings = process_node(nodes)

    joinable_tables = []
    for sib in siblings:
        # Get all the nodes connected to a sibling via RELATED edge
        related_nodes = node_helper.get_joinable(sib['id'])
        print(related_nodes)
        if len(related_nodes) > 0:
            tables = process_relation(related_nodes)
            joinable_tables = joinable_tables + tables

    print(joinable_tables)
    return joinable_tables

def get_siblings(node_id):
    return node_helper.get_siblings(node_id)







