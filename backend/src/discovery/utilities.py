def process_relation(result):
    nodes = []
    for r in result:
        node = list(map(lambda x: {'table': x['source_name'], 'column': x['name'], 'id': x['id']},
                        filter(lambda x: x.get('id') is not None, r.nodes)))[0]
        if len(r.values()) > 0:
            node['sim'] = list(r.values())[0]
        nodes.append(node)
    return nodes


def process_node(result):
    nodes = list(map(lambda x: {'table': x.get('source_name'), 'column': x.get('name'), 'id': x.get('id')}, result))
    return nodes