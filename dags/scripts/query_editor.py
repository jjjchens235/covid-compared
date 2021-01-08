import configparser
import psycopg2
import pandas as pd

config = configparser.ConfigParser()
config.read('dwh.cfg')

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
cur = conn.cursor()

"""

calc = 'sum'
if calc == 'count':
    # staging global less usa
    query = "select count(*) from staging_global_confirmed where country <> 'US'"
    df = pd.read_sql(query, conn)
    print(f'staging global: {df}')

    # staging us
    query = 'select count(*) from staging_us_confirmed'
    df = pd.read_sql(query, conn)
    print(f'us: {df}')

    # confirmed temp
    query = 'select count(*) from confirmed_temp'
    df = pd.read_sql(query, conn)
    print(f'confirmed_temp: {df}')

else:
    # staging global less usa
    query = "select sum(deaths) from staging_global_deaths where country <> 'US'"
    df = pd.read_sql(query, conn)
    print(f'staging global: {df}')

    # staging us
    query = 'select sum(deaths) from staging_us_deaths'
    df = pd.read_sql(query, conn)
    print(f'us: {df}')

    query = 'select sum(deaths) from deaths_temp'
    df = pd.read_sql(query, conn)
    print(f'deaths_temp: {df}')


'''
query  = 'select confirmed_temp.*, concat(county, state, country) from confirmed_temp join location l on confirmed_temp.location_id = l.location_id where l.location_id in (42, 43)'
df = pd.read_sql(query, conn)
'''

query = 'select * from confirmed_temp limit 5'
df = pd.read_sql(query, conn)
print(f'us: {df}')
"""
query = 'select sum(confirmed), sum(deaths), sum(recovered) from fact_metrics'
df = pd.read_sql(query, conn)
print(f'deaths_temp: {df}')

query = "select sum(recovered) from staging_global_recovered where country not in ('US', 'Canada')"
df = pd.read_sql(query, conn)
print(f'deaths_temp: {df}')



query = "SELECT (SELECT sum(confirmed) from staging_global_confirmed)=(SELECT count(*) from table2) AS RowCountResult; "
df = pd.read_sql(query, conn)
print(f'deaths_temp: {df}')
