from os import listdir
from os.path import isfile, join

import pandas as pd
from neo4j import GraphDatabase

from . import node_helper
from . import edge_helper
from .edge_helper import shortest_path_between_tables
from .relation_types import MATCH
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
        # If the node has connection, transform the result into something useful for us
        # { table_name: {PK: { from_id: <id>, to_id: <id> }, RELATED: <threshold>} ... }
        if len(related_nodes) > 0:
            tables = process_relation(table_name, related_nodes)
            joinable_tables = joinable_tables + tables

    return joinable_tables


def delete_spurious_connections():
    # Get all relations ([ [nr of coma properties, relations] ])
    relations = edge_helper.get_related_relations()
    # Filter out the relations with coma (aka Valentine matcher) property
    no_match_relations = list(map(lambda x: x[1], filter(lambda x: x[0] == 0, relations)))
    # For each relation, get the id and delete it
    ids = []
    for relation in no_match_relations:
        ids.append(relation.id)
        edge_helper.delete_relation_by_id(relation.id)
    return ids


def get_related_between_two_tables(from_table: str, to_table: str) -> list:
    # Get all shortest path between the two tables
    paths = shortest_path_between_tables(from_table, to_table)
    all_links = []
    # Each path contains multiple segments
    # A segment is a relationship between two nodes
    for path in paths:
        explanation = f"Table {from_table} and table {to_table} are connected via the following path:"
        link = []
        # Remember the current table, because the traversal is directionless
        # Therefore, we need to find the start node which matches with the previous end node
        current_table = from_table
        for relation in path.relationships:
            # We don't follow the sibling edges, only the match (RELATED)

            if relation.type == MATCH:
                if current_table in relation.start_node['id']:
                    explanation = f"{explanation} {relation.start_node['id']} -> {relation.end_node['id']} ->"
                    link.append(relation.start_node['id'])
                    link.append(relation.end_node['id'])
                    current_table = '/'.join(relation.end_node['id'].split('/')[:-1])
                else:
                    explanation = f"{explanation} {relation.end_node['id']} -> {relation.start_node['id']} ->"
                    link.append(relation.end_node['id'])
                    link.append(relation.start_node['id'])
                    current_table = '/'.join(relation.start_node['id'].split('/')[:-1])
        # Because of the sibling edges, some paths will be similar to previous
        if link not in all_links:
            all_links.append({'explanation': explanation, 'links': link})
    return all_links


def get_siblings(node_id):
    return node_helper.get_siblings(node_id)







