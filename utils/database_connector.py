import mysql.connector
from pymongo import MongoClient
from utils import file_utils
from db_scripts.mysql import sql_scripts
import simplejson as json


def get_mysql_table_row_count(connection_yaml):
    mydb = get_mysql_connection(connection_yaml=connection_yaml)
    mycursor = mydb.cursor()
    mycursor.execute(sql_scripts.queries.GET_TABLE_NAMES,
                     (connection_yaml["database"],))
    db_tables = mycursor.fetchall()
    db_rows = {}
    for table in db_tables:
        mycursor = mydb.cursor()
        mycursor.execute(sql_scripts.queries.GET_ROW_COUNT.format(
            connection_yaml["database"], table[0]))
        db_rows[table[0]] = mycursor.fetchone()[0]
    mydb.close()
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
