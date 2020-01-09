import os
import logging
import datetime
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators import (StreetEasyOperator)

default_args = {
    'owner': 'shravan',
    'depends_on_past': False,
    'provide_context': True,
}

dag = DAG(
        'street_easy',
        default_args=default_args,
        description='Load and Transform street easy data',
        start_date=datetime.utcnow() - timedelta(hours=5),
        schedule_interval='@daily',
        max_active_runs=1
         )

def list_keys(*args, **kwargs):
    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = Variable.get('s3_bucket')
    logging.info(f"Listing Keys from {bucket}")
    keys = hook.list_keys(bucket)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")

start_operator = DummyOperator(task_id='Begin_Execution', dag=dag)

list_task = PythonOperator(
    task_id="list_keys",
    python_callable=list_keys,
    dag=dag
)

extract_and_transform_streeteasy_data = StreetEasyOperator(
    task_id = "extract_and_transform_streeteasy_data",
    dag=dag,
    aws_credentials_id = "aws_credentials",
    s3_bucket = "streeteasy-data-exercise",
    s3_key = "test.csv.gz"
)

end_operator = DummyOperator(task_id='End_Execution', dag=dag)

start_operator >> list_task
list_task >> extract_and_transform_streeteasy_data
extract_and_transform_streeteasy_data >> end_operator
