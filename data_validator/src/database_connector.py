import mysql.connector
from pymongo import MongoClient
import simplejson as json


def get_mysql_table_row_count(connection_yaml):
    mydb = get_mysql_connection(connection_yaml=connection_yaml)
    mycursor = mydb.cursor()
    mycursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s';" %
                     connection_yaml["database"])
    db_tables = mycursor.fetchall()
    db_rows = {}
    for table in db_tables:
        mycursor = mydb.cursor()
        mycursor.execute(
            "SELECT COUNT(*) FROM {0}.{1}".format(connection_yaml["database"], table[0]))
        db_rows[table[0]] = mycursor.fetchone()[0]
    return db_rows


def get_mysql_connection(connection_yaml):
    return mysql.connector.connect(
        host=connection_yaml["host"],
        user=connection_yaml["username"],
        password=connection_yaml["password"],
        database=connection_yaml["database"]
    )


def get_mongodb_documents(mongo_connection_yaml, collection_name):
    client = MongoClient(
        mongo_connection_yaml["host"], mongo_connection_yaml["port"])
    db = client[mongo_connection_yaml["database"]]
    documents = db[collection_name].find()
    document_list = list(documents)
    client.close()
    return document_list


if __name__ == "__main__":
    pass
