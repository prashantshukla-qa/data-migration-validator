import simplejson as json
from datetime import date, datetime
from .mongoimport import mongoDBImport
from src import database_connector
from src.constants import Constants
from src.read_yaml import get_yaml


def serialize_datetime(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type not serializable")


def main():
    database_yaml = get_yaml(Constants.DATABASE_CONFIG_FILENAME)
    mydb = database_connector.get_mysql_connection(database_yaml["mysql"])
    mycursor = mydb.cursor()

    mycursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s';" %
                     database_yaml["mysql"]["database"])

    db_tables = mycursor.fetchall()

    for table in db_tables:
        mycursor.execute(("SELECT * FROM autoretail.%s" % table))
        myresult = mycursor.fetchall()

        customers = []

        for customer_detail in myresult:
            customer_details = {}
            for column_index, column_name in enumerate(mycursor.column_names):
                customer_details[column_name] = customer_detail[column_index]
            customers.append(customer_details)

        mongoDBImport.import_to_mongoDB(table[0], json.dumps(
            customers, default=serialize_datetime, use_decimal=True))
        print("="*10)

