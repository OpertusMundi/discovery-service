from ..clients import mongodb

def get_db():
	return mongodb.get_client()["db"]

def add_table(table_name, column_count, nodes):
	get_db().table_metadata.insert_one({"name": table_name, "column_count": column_count, "nodes": nodes})


def get_table(table_name):
	return get_db().table_metadata.find_one({"name": table_name})

def get_node_ids(table_name):
	return get_table(table_name)["nodes"]

def purge():
	get_db().table_metadata.drop()
