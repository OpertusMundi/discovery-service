import os

from neo4j import GraphDatabase


neo4j_client = None

def get_client():
    global neo4j_client

    if neo4j_client==None:
        address = os.environ["NEO4J_ADDRESS"]
        print(address)
        neo4j_client = GraphDatabase.driver(
            f"neo4j://{address}",
            auth=tuple(os.environ["NEO4J_AUTH"].split("/"))
        )
    return neo4j_client