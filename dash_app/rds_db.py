import psycopg2
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read('config/rds.cfg')
conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['RDS'].values()))
cur = conn.cursor()


def query_rds(query, is_parse_dates=True):
    #option 1
    if is_parse_dates:
        df = pd.read_sql(query, conn, parse_dates={'dt': '%Y-%m-%d'})
    else:
        df = pd.read_sql(query, conn)

    #option 2
    '''
    cur.execute(query)
    res = cur.fetchall()
    cols = []
    for col in cur.description:
        cols.append(col[0])
    df = pd.DataFrame(res, columns=cols)
    '''

    #result = cur.fetchone()[0]
    return df
