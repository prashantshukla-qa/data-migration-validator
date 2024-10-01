from utils.read_yaml import get_yaml
from database_connectors import mysql_conn, mongodb_conn
from utils.constants import Constants
from colorama import Fore, init
from collections import defaultdict
import simplejson as json
from datetime import date, datetime


init(autoreset=True)

mysql_connection_yaml = get_yaml(Constants.DATABASE_CONFIG_FILENAME)["mysql"]
mongodb_connection_yaml = get_yaml(
    Constants.DATABASE_CONFIG_FILENAME)["mongodb"]


def check_for_duplicates():
    primary_keys_from_mysql = mysql_conn.get_mysql_primary_keys(
        mysql_connection_yaml=mysql_connection_yaml)
    for each_key in primary_keys_from_mysql:
        mongodb_conn.check_duplicates_mongodb(
            mongodb_connection_yaml, each_key, primary_keys_from_mysql[each_key])
    


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
        print(Fore.GREEN + "MYSQL table count is same as MongoDB collections count\n")
        print_mysql_mongo_table_details(
            db_table_row_count, mongodb_collections)
    else:
        print(
            Fore.RED + "MYSQL table count does not match to the MONGODB collections count\n")


def check_for_table_schema(test_case):
    if test_case["data-validations"]["target"].lower() == "mongodb":
        print(Fore.YELLOW + "Schema Validation for Mongo DB Target is not feasible")
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


def validate_min_max_presence():
    results = result_tree()
    mongodb_min_max_aggregate = result_tree()
    mysql_min_max_for_all_tables = mysql_conn.get_mysql_min_max_values(
        mysql_connection_yaml)
    # print (json.dumps(mysql_min_max_for_all_tables, default=serialize_datetime, use_decimal=True))
    for table in mysql_min_max_for_all_tables:
        collection_name = table
        # print_json(mysql_min_max_for_all_tables[table])
        for column in mysql_min_max_for_all_tables[table]:
            field_name = column
            # print_json (mysql_min_max_for_all_tables[table][column])
            mongodb_min_max = mongodb_conn.get_min_max_values(
                mongodb_connection_yaml, collection_name, field_name)
            mongodb_min_max_aggregate[collection_name][field_name] = mongodb_min_max
            # if mongodb_min_max["min"] and mongodb_min_max["max"]:
            if not mongodb_conn.is_value_present(mongodb_connection_yaml, collection_name, field_name,  mysql_min_max_for_all_tables[table][column]["min"]):
                results[table][column]["result"] = "Failed"
                results[table][column]["comment"] = "Min"
                results[table][column]["value"]["mongodb"] = mongodb_min_max["min"]
                results[table][column]["value"]["mysql"] = mysql_min_max_for_all_tables[table][column]["min"]
            if not mongodb_conn.is_value_present(mongodb_connection_yaml, collection_name, field_name,  mysql_min_max_for_all_tables[table][column]["max"]):
                results[table][column]["result"] = "Failed"
                results[table][column]["comment"] = "Max"
                results[table][column]["value"]["mongodb"] = mongodb_min_max["max"]
                results[table][column]["value"]["mysql"] = mysql_min_max_for_all_tables[table][column]["max"]
    if results:
        print (Fore.RED + "Min Max validation test failed for following data")
        print (Fore.RED + str(json.dumps(results, default=serialize_datetime, use_decimal=True)))
        


def match_min_max_values():
    results = result_tree()
    mongodb_min_max_aggregate = result_tree()
    mysql_min_max_for_all_tables = mysql_conn.get_mysql_min_max_values(
        mysql_connection_yaml)
    # print (json.dumps(mysql_min_max_for_all_tables, default=serialize_datetime, use_decimal=True))
    for table in mysql_min_max_for_all_tables:
        collection_name = table
        # print_json(mysql_min_max_for_all_tables[table])
        for column in mysql_min_max_for_all_tables[table]:
            field_name = column
            # print_json (mysql_min_max_for_all_tables[table][column])
            mongodb_min_max = mongodb_conn.get_min_max_values(
                mongodb_connection_yaml, collection_name, field_name)
            mongodb_min_max_aggregate[collection_name][field_name] = mongodb_min_max
            if mongodb_min_max["min"] and mongodb_min_max["max"]:
                if not mysql_min_max_for_all_tables[table][column]["min"] == mongodb_min_max["min"]:
                    results[table][column]["result"] = "Failed"
                    results[table][column]["comment"] = "Min"
                    results[table][column]["value"]["mongodb"] = mongodb_min_max["min"]
                    results[table][column]["value"]["mysql"] = mysql_min_max_for_all_tables[table][column]["min"]
                if not mysql_min_max_for_all_tables[table][column]["max"] == mongodb_min_max["max"]:
                    results[table][column]["result"] = "Failed"
                    results[table][column]["comment"] = "Max"
                    results[table][column]["value"]["mongodb"] = mongodb_min_max["max"]
                    results[table][column]["value"]["mysql"] = mysql_min_max_for_all_tables[table][column]["max"]
    # print_json(mongodb_min_max_aggregate)
    print_json(results)


def print_json(dictionary):
    print(json.dumps(dictionary, default=serialize_datetime, use_decimal=True))


def result_tree():
    return defaultdict(result_tree)


def serialize_datetime(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type not serializable")
