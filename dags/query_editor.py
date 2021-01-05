import configparser
import psycopg2
import pandas as pd

config = configparser.ConfigParser()
config.read('dwh.cfg')

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
cur = conn.cursor()


query = 'select * from location'


df = pd.read_sql(query, conn)
print(df.head())
print(df.shape)
