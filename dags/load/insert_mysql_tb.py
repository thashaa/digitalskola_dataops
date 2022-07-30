import pymysql
import re
from airflow.models import Variable

def exec_sql_file(cursor, sql_file):
    print("n[INFO] Executing SQL script file: '%s'" % (sql_file))
    statement = ""

    for line in open(sql_file):
        if re.match(r'--', line):  # ignore sql comment lines
            continue
        if not re.search(r';$', line):  # keep appending lines that don't end in ';'
            statement = statement + line
        else:  # when you get a line ending in ';' then exec statement and reset for next statement
            statement = statement + line
            #print "nn[DEBUG] Executing SQL statement:n%s" % (statement)
            try:
                cursor.execute(statement)
            except (OperationalError, ProgrammingError) as e:
                print("n[WARN] MySQLError during execute statement ntArgs: '%s'" % (str(e.args)))

            statement = ""

def create_table():
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
    #run mysql query
    cur = conn.cursor()
    exec_sql_file(cur,'/home/hadoop/airflow/dags/load/northwind-data.sql')
    conn.commit()
    cur.close()
    conn.close()

    print('table "northwind" successfully inserted')

if __name__=='__main__':
    create_table()
