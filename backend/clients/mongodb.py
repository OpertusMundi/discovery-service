import os

from pymongo import MongoClient

from ..utility.parsing import parse_ip

mongo_client: MongoClient = None


def get_client() -> MongoClient:
    global mongo_client

    if mongo_client is None:
        mongo_client = MongoClient(*parse_ip(os.environ["MONGO_ADDRESS"]))

    return mongo_client
