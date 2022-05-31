from . import relation_types
from ..clients import neo4j as neo


def create_subsumption_relation(source):
    with neo.get_client().session() as session:
        relation = session.write_transaction(_create_subsumption_relation, source)


def create_relation(from_node_id, to_node_id, relation_name):
    with neo.get_client().session() as session:
        relation = session.write_transaction(_create_relation, from_node_id, to_node_id, relation_name)
    return relation


def set_properties(from_node_id, to_node_id, relation_name, **kwargs):
    with neo.get_client().session() as session:
        relation = session.write_transaction(_set_properties, from_node_id, to_node_id, relation_name, **kwargs)
    return relation


def delete_relation_between_nodes(from_node_id, to_node_id, relation_name):
    with neo.get_client().session() as session:
        relation = session.write_transaction(_delete_relation_between_nodes, from_node_id, to_node_id,
                                             relation_name)
    return relation


def delete_relations_by_name(relation_name):
    with neo.get_client().session() as session:
        relations = session.write_transaction(_delete_relations_by_name, relation_name)
    return relations


def get_related_relations():
    with neo.get_client().session() as session:
        relations = session.write_transaction(_get_related_relations)
    return relations


def delete_relation_by_id(relation_id):
    with neo.get_client().session() as session:
        result = session.write_transaction(_delete_relation_by_id, relation_id)
    return result


def shortest_path_between_tables(from_table, to_table):
    with neo.get_client().session() as session:
        result = session.write_transaction(_shortest_path_between_tables, from_table, to_table)
    return result


def _create_subsumption_relation(tx, source):
    tx_result = tx.run("MATCH (a:Node {source_path: $source}) WITH a "
                       "MATCH (b:Node {source_path: $source}) "
                       "WHERE NOT(a.id = b.id) "
                       f"MERGE (a)-[s:{relation_types.SIBLING}]-(b) "
                       "RETURN type(s) as relation", source=source)

    result = []
    for record in tx_result:
        result.append(record['relation'])
    return result


def _create_relation(tx, a_id, b_id, relation_name):
    tx_result = tx.run("MATCH (a:Node {id: $a_id}) WITH a "
                       "MATCH (b:Node {id: $b_id}) "
                       f"MERGE (a)-[r:{relation_name}]-(b) "
                       "RETURN r as relation", a_id=a_id, b_id=b_id)

    result = []
    for record in tx_result:
        result.append(record['relation'])
    return result


def _set_properties(tx, a_id, b_id, relation_name, **kwargs):
    set_query = 'SET '
    for i, key in enumerate(kwargs.keys()):
        set_query += 'r.{} = ${} '.format(key, key)
        if i < len(kwargs.keys()) - 1:
            set_query += ', '

    tx_result = tx.run("MATCH (a:Node)-[r:{}]->(b:Node) WHERE a.id = $a_id and b.id = $b_id {} RETURN r as relation"
                       .format(relation_name, set_query), a_id=a_id, b_id=b_id, **kwargs)
    result = []
    for record in tx_result:
        result.append(record['relation'])
    return result


def _delete_relation_between_nodes(tx, node_id1, node_id2, relation_name):
    result = tx.run("MATCH (a:Node)-[r:{}]->(b:Node) WHERE a.id = $a_id AND b.id = $b_id DELETE r"
                    .format(relation_name), a_id=node_id1, b_id=node_id2)
    return result.single()


def _delete_relations_by_name(tx, relation_name):
    result = tx.run("MATCH (a)-[r]->(b) WHERE type(r) = $relation_name DELETE r", relation_name=relation_name)
    return result.single()


def _get_related_relations(tx):
    tx_result = tx.run("match (n)-[r:RELATED]-(m) return [count(r.coma), r] as result")

    result = []
    for record in tx_result:
        result.append(record['result'])
    return result


def _delete_relation_by_id(tx, relation_id):
    tx_result = tx.run("match ()-[r]-() where id(r)=$relation_id delete r", relation_id=relation_id)
    return tx_result.single()


def _shortest_path_between_tables(tx, from_table, to_table):
    tx_result = tx.run("match (n {source_name: $from_table}), "
                       "(m {source_name: $to_table}), "
                       "p=shortestPath((n)-[r:RELATED|SIBLING*]-(m)) "
                       "return relationships(p) as result", from_table=from_table, to_table=to_table)
    result = []
    for record in tx_result:
        result.append(record['result'])
    return result

