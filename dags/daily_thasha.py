from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

#list data ingest#
data_list = ['orders','order_details']

with DAG('daily_thasha',
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 7, 1)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )    


for data in data_list:
    ingest= BashOperator(
        task_id='ingest_'+data,
        bash_command="""python3 /root/airflow/dags/ingest/thasha/ingest_{{params.data}}.py {{ execution_date.format('YYYY-MM-DD') }}""",
        params = {'data': data}
    )

    to_datalake = BashOperator(
        task_id='to_datalake_'+data,
        bash_command="""gsutil cp /root/output/thasha/{{params.data}}/{{params.data}}.py_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/thasha/staging/{{params.data}}/""",
        params = {'data': data}
    )
    start >> ingest >> to_datalake


