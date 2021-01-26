from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import json
import psycopg2
import re

from scripts.covid_pandas import main


json_path = './dags/config/aws_config.json'
with open(json_path) as file:
    env = json.load(file)
rds = BaseHook.get_connection('rds')

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'jwong',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 25),
    'catchup': False,
    'email': ['justin.wong235@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('covid3', default_args=default_args, schedule_interval='0 13 * * *')


class SQLTemplatedPythonOperator(PythonOperator):
    # Allows sql files to be found
    template_ext = ('.sql',)


def drop_tables(sql_path):
    sql_path = sql_path + '.sql'
    with psycopg2.connect(f"host={rds.host} dbname={rds.schema} user={rds.login} password={rds.password} port={rds.port}") as conn:
        cur = conn.cursor()
        print(f'hello world: {sql_path}')
        with open(sql_path, 'r') as fd:
            sqlfile = fd.read()
            sql_commands = sqlfile.split(';')
            for query in sql_commands:
                query_sub = re.sub('\n', '', query)
                if query_sub:
                    query_sub = query_sub + ';'
                    print(query_sub)
                    cur.execute(query)
                    conn.commit()


def validate_fact_metric(upstream_tables, fact_table, metric):
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
                query = f"SELECT SUM({metric}) FROM {upstream_table} WHERE country <> 'Canada'"
            #confirmed/deaths query
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
        raise ValueError("Diff exceeeded threshhold")


def validate_bi_counts():
    '''
    Check that each bi table (county, state, country) has a distinct count that matches the upstream fact table
    '''

    fact_county_q = 'SELECT count(distinct(combined_key)) FROM fact.fact_metrics f join dim.location l on f.location_id = l.location_id WHERE county is not null'
    fact_state_q = 'SELECT count(distinct(combined_key)) FROM fact.fact_metrics f join dim.location l on f.location_id = l.location_id WHERE state is not null and county is null'
    fact_country_q = 'SELECT count(distinct(combined_key)) FROM fact.fact_metrics f join dim.location l on f.location_id = l.location_id WHERE state is null and county is null'
    bi_county_q = 'select count(distinct combined_key) FROM bi.bi_county'
    bi_state_q = 'select count(distinct combined_key) FROM bi.bi_state'
    bi_country_q = 'select count(distinct combined_key) FROM bi.bi_country'

    def get_diff(bi_table, fact_q, bi_q):
        with psycopg2.connect(f"host={rds.host} dbname={rds.schema} user={rds.login} password={rds.password} port={rds.port}") as conn:
            cur = conn.cursor()
            cur.execute(fact_q)
            fact_count = cur.fetchone()[0]

            cur.execute(bi_q)
            bi_count = cur.fetchone()[0]
            diff = fact_count - bi_count
            print(f"For {bi_table} table...\nfact count: {fact_count}\nbi_count: {bi_count}\nDiff: {diff}\n")
            if diff != 0:
                raise ValueError("Diff exceeeded threshhold")

    get_diff('bi.bi_county', fact_county_q, bi_county_q)
    get_diff('bi.bi_state', fact_state_q, bi_state_q)
    get_diff('bi.bi_country', fact_country_q, bi_country_q)


def deprec_validate_bi_metrics(facts_table, bi_table_type):
    '''
    Check that all the bi tables metrics match upstream
    facts table metrics
    @parameter bi_table_type: county, state, or country
    @facts_table: name of the facts_table to select from
    '''
    metrics = ('confirmed', 'deaths', 'recovered')
    with psycopg2.connect(f"host={rds.host} dbname={rds.schema} user={rds.login} password={rds.password} port={rds.port}") as conn:
        cur = conn.cursor()

        facts_cond = f"WHERE {bi_table_type} is NOT NULL"
        for metric in metrics:
            facts_query = f"SELECT SUM({metric}) FROM {facts_table} join location l on {facts_table}.location_id = l.location_id {facts_cond}"
            cur.execute(facts_query)
            facts_sum = cur.fetchone()[0]

            current_query = f"SELECT SUM({metric}) FROM bi_{bi_table_type}"
            cur.execute(current_query)
            current_sum = cur.fetchone()[0]

            diff = abs(facts_sum - current_sum)
            # check that the sums of fact vs downstream are close
            if diff > 5:
                print(f"For {metric} metric...\nfacts sum: {facts_sum}\nbi_sum: {current_sum}\nDiff: {diff}")
                raise ValueError("Diff exceeeded threshhold")


load_to_s3 = PythonOperator(
    task_id='data_to_s3',
    dag=dag,
    python_callable=main,
    op_args=[env['AWS']['LOGIN'], env['AWS']['PW'], 's3://' + env['S3']['BUCKET'] + '/']
)

# fyi .sql cannot be passed in as args bc of sqlpython operator templating
drop_existing = SQLTemplatedPythonOperator(
    task_id='drop_existing',
    dag=dag,
    python_callable=drop_tables,
    op_args=['./dags/sql/01_drop_existing']
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
    python_callable=validate_fact_metric,
    op_args=[['staging.staging_global_confirmed', 'staging.staging_us_confirmed'], 'fact.fact_metrics', 'confirmed']
)

validate_fact_deaths = PythonOperator(
    task_id='validate_fact_deaths',
    dag=dag,
    python_callable=validate_fact_metric,
    op_args=[['staging.staging_global_deaths', 'staging.staging_us_deaths'], 'fact.fact_metrics', 'deaths']
)

validate_fact_recovered = PythonOperator(
    task_id='validate_fact_recovered',
    dag=dag,
    python_callable=validate_fact_metric,
    op_args=[['staging.staging_global_recovered'], 'fact.fact_metrics', 'recovered']
)

load_bi_tables = PostgresOperator(
    task_id='load_bi_tables',
    dag=dag,
    postgres_conn_id='rds',
    sql='/sql/07_bi_tables_atomic.sql'
)


validate_bi_counts = PythonOperator(
    task_id='validate_bi_counts',
    dag=dag,
    python_callable=validate_bi_counts
)

drop_staging = SQLTemplatedPythonOperator(
    task_id='drop_staging',
    dag=dag,
    python_callable=drop_tables,
    op_args=['./dags/sql/08_drop_staging_tables']
)


load_to_s3 >> drop_existing >> create_tables >> stage_tables >> load_dim_tables >> load_temp_fact_tables >> load_fact_tables >> [validate_fact_confirmed, validate_fact_deaths, validate_fact_recovered] >> load_bi_tables >> validate_bi_counts >> drop_staging
#[load_to_s3, drop_existing >> create_tables] >> stage_tables >> load_dim_tables >> load_temp_fact_tables >> load_fact_tables >> [validate_fact_confirmed, validate_fact_deaths, validate_fact_recovered] >> load_bi_tables >> validate_bi_counts >> drop_staging
