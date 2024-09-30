from utils.read_yaml import get_yaml
from database_connectors import mysql_conn, mongodb_conn
from utils.constants import Constants
from colorama import Fore, init

init(autoreset=True)

mysql_connection_yaml = get_yaml(Constants.DATABASE_CONFIG_FILENAME)["mysql"]
mongodb_connection_yaml = get_yaml(Constants.DATABASE_CONFIG_FILENAME)["mongodb"]

def check_for_duplicates():
    primary_keys_from_mysql = mysql_conn.get_mysql_primary_keys(
        mysql_connection_yaml=mysql_connection_yaml)
    for each_key in primary_keys_from_mysql:
        mongodb_conn.check_duplicates_mongodb(
            mongodb_connection_yaml, each_key, primary_keys_from_mysql[each_key])
    pass

def check_table_row_count():
    """
    This methods validates the table row and count with collections and documents respectively
    """

    db_table_row_count = mysql_conn.get_mysql_table_row_count(
        connection_yaml=mysql_connection_yaml)
    mongodb_collections = mongodb_conn.get_mongodb_collections(
        mongodb_connection_yaml)
    print("There were " + str(len(db_table_row_count)) +
          " tables in " + "mysql" + " database")
    print("There are " + str(len(mongodb_collections)) +
          " documents in " + "mongodb" + " database")
    if len(db_table_row_count) == len(mongodb_collections):
        print(Fore.GREEN + "MYSQL table and row count corresponds to the MONGODB collection and Documents\n")
        print_mysql_mongo_table_details(
            db_table_row_count, mongodb_collections)
    else:
        print(Fore.RED + "MYSQL table and row count does not match to the MONGODB collection and Documents\n")

def check_for_table_schema(test_case):
    if test_case["data-validations"]["target"].lower() == "mongodb":
        print (Fore.YELLOW + "Schema Validation for Mongo DB Target is not feasible")
    else:
        try:
            raise Exception("Schema validation is pendin implementation")
        except Exception as e:
            print(Fore.RED + f"Warning: {e}")

def print_mysql_mongo_table_details(db_table_row_count, mongodb_collections):
    print("Validating individual Table Row Counts!")
    print_tab_width = 25
    print("\tMY_SQL_TABLE_NAME".ljust(print_tab_width) + "ROWS IN MYSQL TABLE".ljust(print_tab_width)
          + "MONGODB COLLECTION NAME".ljust(print_tab_width) + "MONGODB DOCUMENTS IN COLLECTION".ljust(print_tab_width))
    for db_tables in db_table_row_count:
        output_string = ""
        mongodb_documents = mongodb_conn.get_mongodb_documents(
            mongodb_connection_yaml, db_tables)
        output_string = str(db_tables).ljust(print_tab_width) + str(db_table_row_count[db_tables]).ljust(
            print_tab_width) + str(db_tables).ljust(print_tab_width) + str(len(mongodb_documents)).ljust(print_tab_width)
        if db_table_row_count[db_tables] == len(mongodb_documents):
            output_string = Fore.GREEN + output_string
        else:
            print(Fore.RED + "Row count do not match for table :- " + str(db_tables))
            output_string = Fore.RED + output_string
        print("\t" + output_string)
