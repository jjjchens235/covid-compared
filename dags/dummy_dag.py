from airflow import DAG
from airflow.operators import BashOperator, PythonOperator, PostgresOperator
from datetime import datetime, timedelta
import os
import json

fullPath = 'dags/s3_config.json'
with open(fullPath) as file:
    env = json.load(file)
os.environ['S3_BUCKET'] = env['S3_BUCKET']
os.environ['REGION'] = env['REGION']

os.environ['US_CONFIRMED'] = env['US_CONFIRMED']
os.environ['GLOBAL_CONFIRMED'] = env['GLOBAL_CONFIRMED']
os.environ['US_DEATHS'] = env['US_DEATHS']
os.environ['GLOBAL_DEATHS'] = env['GLOBAL_DEATHS']
os.environ['GLOBAL_RECOVERED'] = env['GLOBAL_RECOVERED']

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'jwong',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 6),
    'email': ['justin.wong235@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('Helloworld', default_args=default_args)

'''

conn = psycopg2.connect("host=database-1.clbcztxn9j1s.us-west-1.rds.amazonaws.com dbname=postgres user=postgres password=Yurmom5957 port=5432") #.format(*config['DWH'].values()))
cur = conn.cursor()
'''

drop_tables = PostgresOperator(
    task_id='drop_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='drop_tables.sql'
)

create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='create_tables.sql'
)


stage_tables = PostgresOperator(
    task_id='stage_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='stage_tables.sql',
    params={
        's3_bucket': os.getenv('S3_BUCKET'),
        'region': os.getenv('REGION'),
        'us_confirmed': os.getenv('US_CONFIRMED'),
        'global_confirmed': os.getenv('GLOBAL_CONFIRMED'),
        'us_deaths': os.getenv('US_DEATHS'),
        'global_deaths': os.getenv('GLOBAL_DEATHS'),
        'global_recovered': os.getenv('GLOBAL_RECOVERED')}
)

load_dim_tables = PostgresOperator(
    task_id='load_dim_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='load_dim_tables.sql'
)

'''
load_temp_fact_tables = PostgresOperator(
    task_id='load_temp_fact_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='load_temp_fact_tables.sql'
)

load_fact_table = PostgresOperator(
    task_id='load_fact_table',
    dag=dag,
    postgres_conn_id='rds',
    sql='load_fact_table.sql'
)
'''

drop_tables >> create_tables >> stage_tables >> load_dim_tables # >> load_temp_fact_tables >> load_fact_table


#docker run -d -p 8080:8080 -v /Users/jwong/Programming/Python/DEngineering/covid_de/dags/:/usr/local/airflow/dags  puckel/docker-airflow webserver
