import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators import (StreetEasyOperator, ValidSearchStatsOperator)

# Default arguments for DAG:
# start_date : date from when we need to start processing.
# end_data   : date until when we need to process the data.
# depends_on_past : this DAG is independent of previous runs.
# email_on_retry : We don't want emails on retry.
# retries : Number of times the task is retries upon failure.
# retry_delay : How much time should the scheduler wait before re-attempt.
# provide_context : When you provide_context=True to an operator, we pass
#   along the Airflow context variables to be used inside the operator.
default_args = {
    'owner': 'shravan',
    'start_date': datetime(2018, 1, 20),
    'end_date': datetime(2018, 3, 30),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}

# Dag will start automatically when turned on
# This is because the start date is in the past
dag = DAG(
        'street_easy',
        default_args=default_args,
        description='Load and Transform street easy data',
        schedule_interval='@daily',
        max_active_runs=1
)

def check_connectivity_to_s3(*args, **kwargs):
    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = Variable.get('s3_bucket')
    logging.info(f"Listing Keys from {bucket}")
    keys = hook.list_keys(bucket)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")

start_operator = DummyOperator(task_id='Begin_Execution', dag=dag)

check_connectivity_to_s3 = PythonOperator(
    task_id="check_connectivity_to_s3",
    python_callable=check_connectivity_to_s3,
    dag=dag
)

extract_and_transform_streeteasy_data = StreetEasyOperator(
    task_id = "extract_and_transform_streeteasy_data",
    dag=dag,
    aws_credentials_id = "aws_credentials",
    aws_credentials_dest_id = "aws_credentials_dest",
    s3_bucket = Variable.get('s3_bucket'),
    s3_dest_bucket = Variable.get('s3_dest_bucket'),
    s3_key = "inferred_users.{ds}.csv.gz",
    s3_dest_key = "unique_valid_searches_{ds}.csv",
    s3_dest_df_key = "valid_searches_{ds}.csv",
)

calculate_valid_search_stats = ValidSearchStatsOperator(
    task_id = "calculate_valid_search_stats",
    dag=dag,
    aws_credentials_id = "aws_credentials_dest",
    redshift_conn_id = "redshift",
    table = "search_stats",
    columns = """
        day,
        num_searches,
        num_users,
        num_rental_searches,
        num_sales_searches,
        num_rental_and_sales_searches,
        num_none_type_searches
    """,
    s3_bucket = Variable.get('s3_dest_bucket'),
    s3_key = "valid_searches_{ds}.csv",
    today = "{ds}",
)

end_operator = DummyOperator(task_id='End_Execution', dag=dag)

# DAG layout
start_operator >> check_connectivity_to_s3
check_connectivity_to_s3 >> extract_and_transform_streeteasy_data
extract_and_transform_streeteasy_data >> calculate_valid_search_stats
calculate_valid_search_stats >> end_operator
