from . import edge_helper
from . import node_helper


def create_node(asset_id, table_name, table_path, column_name):
    return node_helper.create_node(asset_id, table_name, table_path, column_name)


def get_nodes():
    return node_helper.get_all()


def get_node_by_prop(**kwargs):
    return node_helper.get_node(**kwargs)


def set_node_properties(node_id, **kwargs):
    return node_helper.set_profiling_props(node_id, **kwargs)


def delete_node_property(node_property, **kwargs):
    return node_helper.delete_property(node_property, **kwargs)


def delete_all_nodes():
    return node_helper.delete_all()


def create_subsumption_relation(source_path):
    return edge_helper.create_subsumption_relation(source_path)


def create_relation(from_node_id, to_node_id, relation_name):
    return edge_helper.create_relation(from_node_id, to_node_id, relation_name)


def set_relation_properties(from_node_id, to_node_id, relation_name, **kwargs):
    return edge_helper.set_properties(from_node_id, to_node_id, relation_name, **kwargs)


def delete_relation_between_nodes(from_node_id, to_node_id, relation_name):
    return edge_helper.delete_relation_between_nodes(from_node_id, to_node_id, relation_name)


def delete_relations_by_name(relation_name):
    return edge_helper.delete_relations_by_name(relation_name)
