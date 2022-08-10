from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta



with DAG('init_thasha',
    schedule_interval="@once",
    start_date=datetime(2022, 7, 6)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )    

for data in ['orders', 'order_details']: 
    ingest = BashOperator(
        task_id='ingest_' +data,
        bash_command="""python3 /root/airflow/dags/ingest/thasha/ingest_{{params.data}}.py {{ execution_date.format('YYYY-MM-DD') }}""",
        params = {'data': data}
    )

    to_datalake= BashOperator(
        task_id='to_datalake_'+data,
        bash_command="""gsutil cp /root/output/thasha/{{params.data}}/{{params.data}}.py_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/thasha/staging/{{params.data}}/""",
        params = {'data': data}
    )
    
    data_definition = BashOperator(
        task_id='data_definition_'+data,
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/thasha/staging/{{params.data}}/* > /root/data_def/thasha/{{params.data}}.def""",
        params = {'data': data}
    )

    to_dwh = BashOperator(
        task_id='to_dwh_'+data,
        bash_command="""bq mk --external_data_definition=/root/data_def/thasha/{{params.data}}.def de_7.thasha_{{params.data}}""",
        params = {'data': data}
    )

for data_one_batch in ['categories','products','customers']:
    ingest_one_batch = BashOperator(
        task_id='ingest_' +data_one_batch,
        bash_command="""python3 /root/airflow/dags/ingest/thasha/ingest_{{params.data_one_batch}}.py""",
        params = {'data_one_batch': data_one_batch}
    )

    to_datalake_one_batch= BashOperator(
        task_id='to_datalake_'+data_one_batch,
        bash_command="""gsutil cp /root/output/thasha/{{params.data_one_batch}}/{{params.data_one_batch}}.csv gs://digitalskola-de-batch7/thasha/staging/{{params.data_one_batch}}/""",
        params = {'data_one_batch': data_one_batch}
    )
    data_definition_one_batch= BashOperator(
        task_id='data_definition_'+data_one_batch,
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/thasha/staging/{{params.data_one_batch}}/* > /root/data_def/thasha/{{params.data_one_batch}}.def""",
        params = {'data_one_batch': data_one_batch}
    )

    to_dwh_one_batch = BashOperator(
        task_id='to_dwh_'+data_one_batch,
        bash_command="""bq mk --external_data_definition=/root/data_def/thasha/{{params.data_one_batch}}.def de_7.thasha_{{params.data_one_batch}}""",
        params = {'data_one_batch': data_one_batch}
    )



    start >> ingest,ingest_one_batch >> to_datalake,to_datalake_one_batch >> data_definition, data_definition_one_batch >> to_dwh, to_dwh_one_batch