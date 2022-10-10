import os

from neo4j import GraphDatabase, Neo4jDriver

neo4j_client: Neo4jDriver = None


def get_client() -> Neo4jDriver:
    global neo4j_client

    if neo4j_client is None:
        address = os.environ["NEO4J_ADDRESS"]
        print(address)
        neo4j_client = GraphDatabase.driver(
            f"neo4j://{address}",
            auth=tuple(os.environ["NEO4J_AUTH"].split("/"))
        )
    return neo4j_client
