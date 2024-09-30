from pymongo import MongoClient
from colorama import Fore, init

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
        print(Fore.RED + f"Found {len(duplicates)} sets of duplicates in collection:- " + collection_name)
        for duplicate in duplicates:
            print(f"\tDuplicate fields: {duplicate['fields']}")
            print(f"\tCount: {duplicate['count']}")
            print(f"\tDocument IDs: {duplicate['ids']}")
    else:
        print(Fore.GREEN + "No duplicates found for the collection:- " + collection_name)
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


if __name__ == "__main__":
    pass
