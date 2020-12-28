import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, count_queries


def load_staging_tables(cur, conn):
    '''
    insert into the two staging tables
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    insert into fact and dim tables
    '''
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def count_tables(cur, conn):
    '''
    analytical queries to make sure row counts are valid
    '''
    for query in count_queries:
        print(query)
        cur.execute(query)
        results = cur.fetchone()
        for row in results:
            print(row)


def main():
    '''
    load, insert, and analyze tables
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    # count_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
