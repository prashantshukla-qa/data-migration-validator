import json
from pymongo import MongoClient
from ...constants import Constants
from ...read_yaml import get_yaml

mongodb_yaml = get_yaml(Constants.DATABASE_CONFIG_FILENAME)["mongodb"]


def import_to_mongoDB(table_name):
    client = MongoClient(mongodb_yaml["host"], mongodb_yaml["port"])
    db = client[mongodb_yaml["database"]]
    collection_currency = db[table_name]

    with open('%s.json' % table_name) as f:
        file_data = json.load(f)
    collection_currency.insert_many(file_data)

    client.close()
    print("Table %s imported to the MongoDB!!" % table_name)


def import_to_mongoDB(table_name, json_data):
    client = MongoClient(mongodb_yaml["host"], mongodb_yaml["port"])
    db = client[mongodb_yaml["database"]]
    collection_autoretail = db[table_name]

    collection_autoretail.insert_many(json.loads(json_data))

    client.close()
    print("Table %s imported to the MongoDB!!" % table_name)


def drop_table():
    client = MongoClient(mongodb_yaml["host"], mongodb_yaml["port"])
    if mongodb_yaml["database"] not in ["admin", "local", "config"]:
        db = client[mongodb_yaml["database"]]
        db.drop_collection()
    else:
        raise Exception(
            "MongoDB core databse e.g. admin, local, config can not be droped. You have requested to drop %s. Please check." % mongodb_yaml["database"])
    client.close()
    print("Collection %s Has BEEN DELETED!!" % mongodb_yaml["database"])
