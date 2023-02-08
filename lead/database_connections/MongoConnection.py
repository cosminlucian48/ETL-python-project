from pymongo import MongoClient

from lead.db_helper.db_credentials_helper import get_mongo_creds


class MongoConnection:
    def __init__(self):
        config = get_mongo_creds()

        if config:
            client = MongoClient(
                f"mongodb://{config['user']}:{config['password']}@{config['endpoint']}/{config['database']}")
            db = client[f"{config['database']}"]
            self.collection = db[f"{config['collection']}"]

        else:
            raise Exception("Could not connect to the Mongo DB.")
