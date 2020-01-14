from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator

default_args = {
    'owner': 'shravan',
    'start_date': datetime.utcnow() - timedelta(hours=5),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup_by_default': False,
}

dag = DAG('create_search_tables_dag',
          default_args=default_args,
          description='Create tables in Redshift using Airflow',
          schedule_interval=None,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_search_stats_table = PostgresOperator(
    task_id="create_search_stats_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql='''
    CREATE TABLE IF NOT EXISTS public.search_stats (
    	day date,
    	num_searches int,
    	num_users int,
        num_rental_searches int,
        num_sales_searches int,
        num_rental_and_sales_searches int,
        num_none_type_searches int
    );
    '''
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# task dependencies
start_operator >> create_search_stats_table
create_search_stats_table >> end_operator
