from utils.read_yaml import get_yaml
from utils import database_connector
from utils.constants import Constants
from colorama import Fore, init

init(autoreset=True)


mysql_connection_yaml = get_yaml(Constants.DATABASE_CONFIG_FILENAME)["mysql"]
mongodb_connection_yaml = get_yaml(Constants.DATABASE_CONFIG_FILENAME)["mongodb"]


def check_table_row_count():
    """
    This methods validates the table row and count with collections and documents respectively
    """

    db_table_row_count = database_connector.get_mysql_table_row_count(
        connection_yaml=mysql_connection_yaml)
    mongodb_collections = database_connector.get_mongodb_collections(
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


def print_mysql_mongo_table_details(db_table_row_count, mongodb_collections):
    print("Validating individual Table Row Counts!")
    print_tab_width = 25
    print("MY_SQL_TABLE_NAME".ljust(print_tab_width) + "ROWS IN MYSQL TABLE".ljust(print_tab_width)
          + "MONGODB COLLECTION NAME".ljust(print_tab_width) + "MONGODB DOCUMENTS IN COLLECTION".ljust(print_tab_width))
    for db_tables in db_table_row_count:
        mongodb_documents_size = database_connector.get_mongodb_documents(
            mongodb_connection_yaml, db_tables)
        print(str(db_tables).ljust(print_tab_width) + str(db_table_row_count[db_tables]).ljust(print_tab_width)
              + str(db_tables).ljust(print_tab_width) + str(len(mongodb_documents_size)).ljust(print_tab_width))
