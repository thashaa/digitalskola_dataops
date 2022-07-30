from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('insert_syarif',
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 7, 1)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )    
      
    ingest_orders = BashOperator(
        task_id='ingest_orders',
        bash_command="""python3 /home/hadoop/airflow/dags/ingest/syarif/ingest_orders.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    start >> ingest_orders