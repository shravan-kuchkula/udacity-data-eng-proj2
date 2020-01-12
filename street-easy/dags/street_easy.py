import os
import logging
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
    'start_date': datetime(2018, 1, 20),
    'end_date': datetime(2018, 2, 1),
    'depends_on_past': False,
    'provide_context': True,
}

# s3://streeteasy-data-exercise/inferred_users.201832018-03-11.csv.gz
# s3://streeteasy-data-exercise/inferred_users.20180120.csv.gz
# s3://streeteasy-data-exercise/inferred_users.20180120.csv.gz

dag = DAG(
        'street_easy',
        default_args=default_args,
        description='Load and Transform street easy data',
        #start_date=datetime.utcnow() - timedelta(hours=5),
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
    aws_credentials_dest_id = "aws_credentials_dest",
    s3_bucket = "streeteasy-data-exercise",
    s3_dest_bucket = "skuchkula-etl",
    s3_key = "inferred_users.{ds}.csv.gz",
    s3_dest_key = "valid_searches.{ds}.csv.gz",
)

end_operator = DummyOperator(task_id='End_Execution', dag=dag)

start_operator >> list_task
list_task >> extract_and_transform_streeteasy_data
extract_and_transform_streeteasy_data >> end_operator
