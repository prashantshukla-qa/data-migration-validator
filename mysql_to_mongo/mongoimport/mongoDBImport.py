import json
from pymongo import MongoClient


def import_to_mongoDB(table_name):
    client = MongoClient('localhost', 27017)
    db = client['AutoRetail']
    collection_currency = db[table_name]

    with open('%s.json' % table_name) as f:
        file_data = json.load(f)
    collection_currency.insert_many(file_data)

    client.close()
    print("Table %s imported to the MongoDB!!" % table_name)


def import_to_mongoDB(table_name, json_data):
    client = MongoClient('localhost', 27017)
    db = client['AutoRetail']
    collection_currency = db[table_name]

    collection_currency.insert_many(json.loads(json_data))

    client.close()
    print("Table %s imported to the MongoDB!!" % table_name)
