#docker run -d -p 8080:8080 -v /Users/jwong/Programming/Python/DEngineering/covid_de/dags/:/usr/local/airflow/dags 74a09a6af034 webserver

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import os
import json
import psycopg2
import re

from scripts.covid_pandas import main

json_path = './dags/config/s3_config.json'
with open(json_path) as file:
    env = json.load(file)
os.environ['S3_BUCKET'] = env['S3_BUCKET']
os.environ['REGION'] = env['REGION']

os.environ['US_CONFIRMED'] = env['US_CONFIRMED']
os.environ['GLOBAL_CONFIRMED'] = env['GLOBAL_CONFIRMED']
os.environ['US_DEATHS'] = env['US_DEATHS']
os.environ['GLOBAL_DEATHS'] = env['GLOBAL_DEATHS']
os.environ['GLOBAL_RECOVERED'] = env['GLOBAL_RECOVERED']


rds = BaseHook.get_connection('rds')
aws = BaseHook.get_connection('aws_default')

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'jwong',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 10),
    'email': ['justin.wong235@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('covid', default_args=default_args)


class SQLTemplatedPythonOperator(PythonOperator):
    # Allows sql files to be found
    template_ext = ('.sql',)


def drop_tables(sql_path):
    with psycopg2.connect(f"host={rds.host} dbname={rds.schema} user={rds.login} password={rds.password} port={rds.port}") as conn:
        cur = conn.cursor()
        print(f'hello world: {sql_path}.sql')
        with open(sql_path + '.sql', 'r') as fd:
            sqlfile = fd.read()
            sql_commands = sqlfile.split(';')
            for query in sql_commands:
                query_sub = re.sub('\n', '', query)
                if query_sub:
                    query_sub = query_sub + ';'
                    print(query_sub)
                    cur.execute(query)
                    conn.commit()


def validate_metric(upstream_tables, fact_table, metric):
    #for each upstream table (staging us/staging global), we want to get increment the sumof the upstream total
    #Then we compare it against the fact table metric

    with psycopg2.connect(f"host={rds.host} dbname={rds.schema} user={rds.login} password={rds.password} port={rds.port}") as conn:
        cur = conn.cursor()

        upstream_sum = 0
        fact_sum = 0
        # get the sum of the upstream staging tables
        for upstream_table in upstream_tables:
            #recovered query
            if metric == 'recovered':
                query = f"SELECT SUM({metric}) FROM {upstream_table} WHERE country not in ('US', 'Canada')"
            #confirmed/deaths query
            else:
                #global query
                if 'global' in upstream_table:
                    query = f"SELECT SUM({metric}) FROM {upstream_table} WHERE country not in ('US')"
                #us query
                else:
                    query = f"SELECT SUM({metric}) FROM {upstream_table}"
            cur.execute(query)
            upstream_sum += cur.fetchone()[0]

        # get the sum of the fact table
        query = f"SELECT SUM({metric}) FROM {fact_table}"
        cur.execute(query)
        fact_sum = cur.fetchone()[0]

    diff = abs(upstream_sum - fact_sum)
    print(f"For {metric} metric...\nupstream sum: {upstream_sum}\nfact_sum: {fact_sum}\nDiff: {diff}")
    # check that the sums of fact vs downstream are close
    if diff > 5:
        raise ValueError("Diff exceeeded threahhold")


load_to_s3 = PythonOperator(
    task_id='data_to_s3',
    dag=dag,
    python_callable=main,
    op_args=[aws.login, aws.password]
)


drop_existing = SQLTemplatedPythonOperator(
    task_id='drop_existing',
    dag=dag,
    python_callable=drop_tables,
    op_args=['./dags/sql/drop_tables']
)

create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/create_tables.sql'
)


stage_tables = PostgresOperator(
    task_id='stage_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/stage_tables.sql',
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
    sql='/sql/load_dim_tables.sql'
)

'''
load_temp_fact_tables = PostgresOperator(
    task_id='load_temp_fact_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='load_temp_fact_tables.sql'
)

'''

load_confirmed_temp = PostgresOperator(
    task_id='load_confirmed_temp',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/load_confirmed_temp.sql'
)


load_deaths_temp = PostgresOperator(
    task_id='load_deaths_temp',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/load_deaths_temp.sql'
)

load_recovered_temp = PostgresOperator(
    task_id='load_recovered_temp',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/load_recovered_temp.sql'
)

load_fact_table = PostgresOperator(
    task_id='load_fact_table',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/load_fact_table.sql'
)

validate_confirmed = PythonOperator(
    task_id='validate_confirmed',
    dag=dag,
    python_callable=validate_metric,
    op_args=[['staging_global_confirmed', 'staging_us_confirmed'], 'fact_metrics', 'confirmed']
)

validate_deaths = PythonOperator(
    task_id='validate_deaths',
    dag=dag,
    python_callable=validate_metric,
    op_args=[['staging_global_deaths', 'staging_us_deaths'], 'fact_metrics', 'deaths']
)

validate_recovered = PythonOperator(
    task_id='validate_recovered',
    dag=dag,
    python_callable=validate_metric,
    op_args=[['staging_global_recovered'], 'fact_metrics', 'recovered']
)

drop_temp = SQLTemplatedPythonOperator(
    task_id='drop_temp',
    dag=dag,
    python_callable=drop_tables,
    op_args=['./dags/sql/drop_temp_tables']
)


[load_to_s3, drop_existing >> create_tables] >> stage_tables >> load_dim_tables >> load_confirmed_temp >> load_deaths_temp >> load_recovered_temp >> load_fact_table >> [validate_confirmed, validate_deaths, validate_recovered] >> drop_temp
