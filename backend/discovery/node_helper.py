from . import relation_types
from ..clients import neo4j as neo


def create_node(source, source_path, label):
    with neo.get_client().session() as session:
        node = session.write_transaction(_create_node, source, source_path, label)
    return node


def set_profiling_props(node_id, **kwargs):
    with neo.get_client().session() as session:
        node = session.write_transaction(_set_profiling_properties, node_id, **kwargs)
    return node


def get_all():
    with neo.get_client().session() as session:
        nodes = session.write_transaction(_get_all)
    return nodes


def get_node(**kwargs):
    with neo.get_client().session() as session:
        node = session.write_transaction(_get_node, **kwargs)
    return node


def get_related_nodes(node_id):
    with neo.get_client().session() as session:
        nodes = session.write_transaction(_get_related_nodes, node_id)
    return nodes


def get_joinable(node_id):
    with neo.get_client().session() as session:
        nodes = session.write_transaction(_get_joinable, node_id)
    return nodes


def get_siblings(node_id):
    with neo.get_client().session() as session:
        nodes = session.write_transaction(_get_siblings, node_id)
    return nodes


def get_nodes_by_table_path(source_path):
    with neo.get_client().session() as session:
        nodes = session.write_transaction(_get_nodes_by_table_path, source_path)
    return nodes


def delete_property(property_to_delete, **kwargs):
    with neo.get_client().session() as session:
        node = session.write_transaction(_delete_property, property_to_delete, **kwargs)
    return node


def delete_all_properties(node_id):
    with neo.get_client().session() as session:
        node = session.write_transaction(_delete_all_properties, node_id)
    return node


def delete_relation(node_id, relation):
    with neo.get_client().session() as session:
        node = session.write_transaction(_delete_relation_from_node, node_id, relation)
    return node


def delete_node_and_all_relations(node_id):
    with neo.get_client().session() as session:
        node = session.write_transaction(_delete_node_and_all_relations, node_id)
    return node


def delete_all():
    with neo.get_client().session() as session:
        node = session.write_transaction(_delete_all)
    return node


def _get_nodes_by_table_path(tx, source_path):
    tx_result = tx.run("MATCH (n) "
                       "WHERE n.source_path = $source_path "
                       "RETURN n as result", source_path=source_path)

    result = []
    for record in tx_result:
        result.append(record['result'])
    return result


def _get_siblings(tx, node_id):
    tx_result = tx.run(f"MATCH (a:Node)-[r:{relation_types.SIBLING}]-(b:Node) "
                       "WHERE a.id = $node_id "
                       "RETURN b as result", node_id=node_id)

    result = []
    for record in tx_result:
        result.append(record['result'])
    return result


def _get_joinable(tx, node_id):
    tx_result = tx.run(f"MATCH (a:Node)-[r:{relation_types.FOREIGN_KEY_IND}]-(b:Node) "
                       "WHERE a.id = $node_id "
                       "RETURN b, r as result", node_id=node_id)

    result = []
    for record in tx_result:
        result.append(record['result'])
    return result


def _get_related_nodes(tx, node_id):
    tx_result = tx.run("MATCH (a:Node)-[r:MATCH]-(b:Node) "
                       "WHERE a.id = $node_id "
                       "RETURN b, r as result", node_id=node_id)
    result = []
    for record in tx_result:
        result.append(record['result'])
    return result


def _create_node(tx, source, source_path, label):
    tx_result = tx.run("CREATE (n:Node) "
                       "SET n.id = $source_path + '/' + $label, "
                       "n.name = $label, "
                       "n.source_name = $source, "
                       "n.source_path = $source_path "
                       "RETURN n as node", source=source, label=label, source_path=source_path)
    result = []
    for record in tx_result:
        result.append(record['node'])
    return result


def _set_profiling_properties(tx, node_id, **kwargs):
    set_query = 'SET '
    for i, key in enumerate(kwargs.keys()):
        set_query += 'n.{} = ${} '.format(key, key)
        if i < len(kwargs.keys()) - 1:
            set_query += ', '

    tx_result = tx.run("MATCH (n:Node) WHERE n.id = $id {} RETURN n as node".format(set_query), id=node_id, **kwargs)
    result = []
    for record in tx_result:
        print(record)
        result.append(record['node'])
    return result


def _get_all(tx):
    result = tx.run("MATCH (n) RETURN n as nodes")
    nodes = []
    for record in result:
        nodes.append(record['nodes'])
    return nodes


def _get_node(tx, **kwargs):
    where_query = ''
    for i, key in enumerate(kwargs.keys()):
        where_query += 'WHERE n.{} = ${} '.format(key, key)
        if i < len(kwargs.keys()) - 1:
            where_query += ' AND '

    tx_result = tx.run("MATCH (n:Node) {} RETURN n as node".format(where_query), **kwargs)

    result = []
    for record in tx_result:
        result.append(record['node'])

    return result


def _delete_property(tx, remove_prop, **kwargs):
    where_query = ''
    for i, key in enumerate(kwargs.keys()):
        where_query += 'WHERE n.{} = ${} '.format(key, key)
        if i < len(kwargs.keys()) - 1:
            where_query += ' AND '

    tx_result = tx.run("MATCH (n:Node) {} REMOVE n.{} RETURN n as node".format(where_query, remove_prop), **kwargs)
    result = []
    for record in tx_result:
        result.append(record['node'])
    return result


def _delete_all_properties(tx, node_id):
    result = tx.run("MATCH (n {id: $id}) "
                    "SET n = {} "
                    "RETURN n", id=node_id)
    return result.single()


def _delete_relation_from_node(tx, node_id, relation):
    result = tx.run("MATCH (n {id: $id})-[r:$relation]->()"
                    "DELETE r", id=node_id, relation=relation)
    return result.single()


def _delete_node_and_all_relations(tx, node_id):
    result = tx.run("MATCH (n {id: $id})"
                    "DETACH DELETE n", id=node_id)
    return result.single()


def _delete_all(tx):
    result = tx.run("MATCH (n)"
                    "DETACH DELETE n")
    return result.single()
