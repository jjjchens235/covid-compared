from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow import settings
from airflow.models import Connection

from datetime import datetime, timedelta
import json

from scripts.covid_pandas import main
from helpers import validation_helper


json_path = './dags/config/aws_config.json'
with open(json_path) as file:
    env = json.load(file)

default_args = {
    'owner': 'jwong',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 5),
    'catchup': False,
    'email': ['justin.wong235@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG('covid1', default_args=default_args, schedule_interval='0 13 * * *')


def create_conn(conn_id, conn_type, host, schema, login, password, port):
    """ Create connection in airflow"""
    session = settings.Session()
    #create a connection object
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        schema=schema,
        login=login,
        password=password,
        port=port
    )
    session = settings.Session() # get the session
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

    if str(conn_name) == str(conn_id):
        print(f"Connection {conn_id} already exists")
        return
    session.add(conn)
    session.commit()


create_rds_conn = PythonOperator(
    dag=dag,
    task_id='create_rds_conn',
    python_callable=create_conn,
    op_kwargs={'conn_id': 'rds', 'conn_type': env['RDS']['TYPE'], 'host': env['RDS']['HOST'], 'schema': env['RDS']['SCHEMA'], 'login': env['RDS']['LOGIN'], 'password': env['RDS']['PASSWORD'], 'port': env['RDS']['PORT']}
)

#pandas script that moves data in github to S3 csv file
load_to_s3 = PythonOperator(
    task_id='data_to_s3',
    dag=dag,
    python_callable=main,
    op_args=[env['AWS']['LOGIN'], env['AWS']['PW'], 's3://' + env['S3']['BUCKET'] + '/']
)


'''
drop_existing = PostgresOperator(
    task_id='drop_existing',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/01_drop_existing.sql'
)
'''

drop_staging = PostgresOperator(
    task_id='drop_staging',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/08_drop_staging_tables.sql'
)

create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/02_create_tables.sql'
)


stage_tables = PostgresOperator(
    task_id='stage_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/03_stage_tables.sql',
    params={
        's3_bucket': env['S3']['BUCKET'],
        'region': env['S3']['REGION'],
        'us_confirmed': env['S3']['US_CONFIRMED'],
        'global_confirmed': env['S3']['GLOBAL_CONFIRMED'],
        'us_deaths': env['S3']['US_DEATHS'],
        'global_deaths': env['S3']['GLOBAL_DEATHS'],
        'global_recovered': env['S3']['GLOBAL_RECOVERED'],
        'location': env['S3']['LOCATION']
    }
)

load_dim_tables = PostgresOperator(
    task_id='load_dim_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/04_load_dim_tables.sql'
)

load_temp_fact_tables = PostgresOperator(
    task_id='load_temp_fact_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/05_load_temp_fact_tables.sql'
)

load_fact_tables = PostgresOperator(
    task_id='load_fact_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/06_load_fact_tables.sql'
)

validate_fact_confirmed = PythonOperator(
    task_id='validate_fact_confirmed',
    dag=dag,
    python_callable=validation_helper.validate_fact_metric,
    op_args=[['staging.staging_global_confirmed', 'staging.staging_us_confirmed'], 'fact.fact_metrics', 'confirmed']
)

validate_fact_deaths = PythonOperator(
    task_id='validate_fact_deaths',
    dag=dag,
    python_callable=validation_helper.validate_fact_metric,
    op_args=[['staging.staging_global_deaths', 'staging.staging_us_deaths'], 'fact.fact_metrics', 'deaths']
)

validate_fact_recovered = PythonOperator(
    task_id='validate_fact_recovered',
    dag=dag,
    python_callable=validation_helper.validate_fact_metric,
    op_args=[['staging.staging_global_recovered'], 'fact.fact_metrics', 'recovered']
)

#population thresh is based off of median (50th percentile) populations for respective location level
load_bi_tables = PostgresOperator(
    task_id='load_bi_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/07_bi_tables_atomic.sql',
    params={
        'county_thresh': 26000,
        'state_thresh': 1800000,
        'country_thresh': 9000000
    }
)

validate_bi_counts = PythonOperator(
    task_id='validate_bi_counts',
    dag=dag,
    python_callable=validation_helper.validate_bi_counts
)


load_to_s3 >> create_rds_conn >> drop_staging >> create_tables >> stage_tables >> load_dim_tables >> load_temp_fact_tables >> load_fact_tables >> [validate_fact_confirmed, validate_fact_deaths, validate_fact_recovered] >> load_bi_tables >> validate_bi_counts
