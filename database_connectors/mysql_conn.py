import mysql.connector
from db_scripts.mysql import sql_scripts
from collections import defaultdict


def get_mysql_table_row_count(connection_yaml):
    """
    return a dictionary of tablename and respective row counts in the table
    """
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


def get_mysql_primary_keys(mysql_connection_yaml):
    mysql_db = get_mysql_connection(connection_yaml=mysql_connection_yaml)
    mycursor = mysql_db.cursor(buffered=True)
    # mycursor.execute(sql_scripts.queries.GET_TABLE_NAMES,
    #                  (mysql_connection_yaml["database"],))
    # db_tables = mycursor.fetchall()
    primary_keys = defaultdict(list)
    mycursor.execute(sql_scripts.queries.GET_PRIMARY_KEYS,
                     (mysql_connection_yaml["database"],))
    results = mycursor.fetchall()
    for each_result in results:
        primary_keys[each_result[0]].append(each_result[1])
    mysql_db.close()
    return primary_keys


def get_mysql_connection(connection_yaml):
    return mysql.connector.connect(
        host=connection_yaml["host"],
        user=connection_yaml["username"],
        password=connection_yaml["password"],
        database=connection_yaml["database"]
    )

def get_mysql_min_max_values(mysql_connection_yaml):
    try:
        table_schema = {}
        min_max_for_each_table = {}
        mysql_db = get_mysql_connection(connection_yaml=mysql_connection_yaml)
        mycursor = mysql_db.cursor(buffered=True)
        mycursor.execute(sql_scripts.queries.GET_TABLE_NAMES,
                     (mysql_connection_yaml["database"],))
        db_tables = mycursor.fetchall()
        for each_table in db_tables:
            min_max_for_each_column = {}
            mycursor.execute("SHOW COLUMNS FROM autoretail.{}".format(each_table[0]))
            columns = [row[0] for row in mycursor.fetchall()]
            table_schema[each_table] = columns
            for column in columns:
                query = f"SELECT MIN({column}), MAX({column}) FROM {each_table[0]}"
                mycursor.execute(query)
                result = mycursor.fetchone()
                min_max_for_each_column[column] = {'min': result[0], 'max': result[1]}
            min_max_for_each_table[each_table[0]] = min_max_for_each_column
        return min_max_for_each_table
    finally:
        if mysql_db:
            mysql_db.close()
