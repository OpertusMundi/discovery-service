def process_relation(result):
    tables = []
    for relation in result:
        table = {}
        # Get connected node to the sibling. A relation is only between 2 nodes, so filter outputs one result
        related_node = list(filter(lambda x: x.get('id') is not None, relation.nodes))[0]

        table['table_name'] = related_node.get('source_name')
        if "to_id" in relation:
            table["PK"] = {'from_id': relation['from_id'], 'to_id': relation['to_id']}
        if "coma" in relation:
            table["RELATED"] = {'coma': relation['coma']}

        tables.append(table)
    return tables


def process_node(result):
    return list(map(lambda x: {'table': x.get('source_path'), 'column': x.get('name'), 'id': x.get('id')}, result))
