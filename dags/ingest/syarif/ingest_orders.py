import pymysql
import csv
import sys
from airflow.models import Variable

mysql_host = Variable.get("mysql_host")
mysql_port = int(Variable.get("mysql_port"))
mysql_user = Variable.get("mysql_user")
mysql_password = Variable.get("mysql_password")
mysql_db = Variable.get("mysql_db")

conn = pymysql.connect(host=mysql_host,
                           port=mysql_port,
                           user=mysql_user, 
                           password=mysql_password,  
                           db=mysql_db)
cur = conn.cursor()


sql = """select * from northwind.orders o where cast(order_date as date) = '"""+sys.argv[1]+"""'"""
csv_file_path = '/home/hadoop/output/syarif/orders/orders_'+sys.argv[1]+'.csv'

try:
    cur.execute(sql)
    rows = cur.fetchall()
finally:
    conn.close()

# Continue only if there are rows returned.
if rows:
    # New empty list called 'result'. This will be written to a file.
    result = list()

    # The row name is the first entry for each entity in the description tuple.
    column_names = list()
    for i in cur.description:
        column_names.append(i[0])

    result.append(column_names)
    for row in rows:
        result.append(row)

    # Write result to file.
    with open(csv_file_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in result:
            csvwriter.writerow(row)
else:
    print("No rows found for query: {}".format(sql))
    