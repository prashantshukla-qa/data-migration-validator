import mysql.connector
import simplejson as json
from datetime import date, datetime
from mongoimport import mongoDBImport


def serialize_datetime(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type not serializable")


def main():

    mydb = mysql.connector.connect(
        host="localhost",
        user="prashant",
        password="Qait@123",
        database="autoretail"
    )

    mycursor = mydb.cursor()

    mycursor.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'autoretail';")

    db_tables = mycursor.fetchall()

    for table in db_tables:
        print("="*10)

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

if __name__ == "__main__":
    main()
