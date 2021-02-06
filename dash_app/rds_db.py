"""
Queries AWS RDS database and returns the result as a pandas dataframe, to be used by Dash.
"""
import psycopg2
import configparser
import pandas as pd
import tempfile
import os

config = configparser.ConfigParser()
config.read('config/dash_app.cfg')


def query_rds(query, is_tmp_file=True):
    #print(query)
    with psycopg2.connect(f"host={os.environ['RDS_HOST']} dbname={os.environ['RDS_DB']} user={os.environ['RDS_USER']} password={os.environ['RDS_PW']} port={os.environ['RDS_PORT']}") as conn:
        if is_tmp_file:
            return query_tmp_file(query, conn)
        else:
            return query_read_sql(query, conn)


def query_tmp_file(query, conn):
    """
    slightly better performance than native pd.read_sql
    """
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(query=query, head="HEADER")
        cur = conn.cursor()
        cur.copy_expert(copy_sql, tmpfile)
        tmpfile.seek(0)
        df = pd.read_csv(tmpfile)
    return df


def query_read_sql(query, conn):
    df = pd.read_sql(query, conn)
    return df
