from pymongo import MongoClient
from colorama import Fore, init
import decimal
import datetime

init(autoreset=True)


def check_duplicates_mongodb(mongo_connection_yaml, collection_name, primary_fields):
    client = MongoClient(
        mongo_connection_yaml["host"], mongo_connection_yaml["port"])
    db = client[mongo_connection_yaml["database"]]
    collection = db[collection_name]

    group_stage = {
        "_id": {field: f"${field}" for field in primary_fields},
        "count": {"$sum": 1},
        "ids": {"$push": "$_id"}
    }
    pipeline = [
        {"$group": group_stage},
        {"$match": {"count": {"$gt": 1}}},
        {"$project": {
            "_id": 0,
            "fields": "$_id",
            "count": 1,
            "ids": 1
        }}
    ]
    duplicates = list(collection.aggregate(pipeline))
    if duplicates:
        print(
            Fore.RED + f"\tFound {len(duplicates)} sets of duplicates in collection:- " + collection_name)
        for duplicate in duplicates:
            print(f"\t\tDuplicate fields: {duplicate['fields']}")
            print(f"\t\tCount: {duplicate['count']}")
            print(f"\t\tDocument IDs: {duplicate['ids']}")
    else:
        print(Fore.GREEN + "\tNo duplicates found for the collection:- " + collection_name)
    client.close()


def get_mongodb_documents(mongo_connection_yaml, collection_name):
    client = MongoClient(
        mongo_connection_yaml["host"], mongo_connection_yaml["port"])
    db = client[mongo_connection_yaml["database"]]
    documents = db[collection_name].find()
    document_list = list(documents)
    client.close()
    return document_list


def get_mongodb_collections(mongo_connection_yaml):
    client = MongoClient(
        mongo_connection_yaml["host"], mongo_connection_yaml["port"])
    db = client[mongo_connection_yaml["database"]]
    collections = db.list_collection_names()
    client.close()
    return collections


def is_value_present(mongodb_connection_yaml, collection_name, field_name, value):
    value = float(value) if type(value) == decimal.Decimal else value
    value = str(value) if type(value) == datetime.date else value
    client = MongoClient(
        mongodb_connection_yaml["host"], mongodb_connection_yaml["port"])
    db = client[mongodb_connection_yaml["database"]]
    collection = db[collection_name]
    # Query to check if field equals the value
    query = {field_name: {"$eq": value}}
    result = collection.find_one(query)
    client.close()
    return result is not None

def get_min_max_values(mongodb_connection_yaml, collection_name, field_name):
    client = MongoClient(
        mongodb_connection_yaml["host"], mongodb_connection_yaml["port"])
    db = client[mongodb_connection_yaml["database"]]
    collection = db[collection_name]
    # client.close()
    return __get_min_max(collection, field_name)


def __get_min_max(collection, field_name):
    return {
        'min': __get_min_value(collection, field_name),
        'max': __get_max_value(collection, field_name)
    }


def __get_min_value(collection, field_name):
    min_value = collection.find_one(
        {}, {field_name: 1}, sort=[(field_name, 1)])
    return min_value.get(field_name) if min_value else None


def __get_max_value(collection, field_name):
    max_value = collection.find_one(
        {}, {field_name: 1}, sort=[(field_name, -1)])
    return max_value.get(field_name) if max_value else None
