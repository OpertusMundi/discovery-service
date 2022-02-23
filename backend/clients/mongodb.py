import pymongo
import os

from ..utility.parsing import parse_ip


mongo_client = None


def get_client():
    global mongo_client

    if mongo_client is None:
        mongo_client = pymongo.MongoClient(*parse_ip(os.environ["MONGO_ADDRESS"]))

    return mongo_client
