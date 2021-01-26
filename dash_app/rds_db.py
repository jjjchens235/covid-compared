import psycopg2
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read('config/dash_app.cfg')


def query_rds(query, is_parse_dates=True):
    #option 1
    with psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['RDS'].values())) as conn:
        if is_parse_dates:
            df = pd.read_sql(query, conn, parse_dates={'dt': '%Y-%m-%d'})
        else:
            df = pd.read_sql(query, conn)
    return df
