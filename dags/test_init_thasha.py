from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import subprocess

def ingest():
    file_list=["ingest_orders.py", "ingest_order_details.py", "ingest_categories.py", "ingest_products.py", "ingest_customers.py"]
    for file in file_list:
        if file == "ingest_orders.py" or file =="ingest_order_details.py":
            f=subprocess.run(["python3 /root/airflow/dags/ingest/thasha/"+(file)+"{{ execution_date.format('YYYY-MM-DD') }}"])
        else :
            f1=subprocess.run(["python3"+(file)])

with DAG('init_thasha',
    schedule_interval="@once",
    start_date=datetime(2022, 7, 6)     
) as dag:

    start = DummyOperator(
        task_id='start'
    )    

    ingest_orders = PythonOperator(
        task_id='ingest_',
        python_callable =ingest
    )
    
    start >> ingest_orders 