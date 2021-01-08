import configparser
import psycopg2
from sql_queries import aws_s3_extension, copy_table_queries, insert_table_queries, count_queries, sum_confirmed, sum_deaths, sum_recovered, isnull_queries


def load_staging_tables(cur, conn):
    '''
    insert into the two staging tables
    '''
    for query in copy_table_queries:
        print(query)
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


count_staging_us_confirmed = ("""
        SELECT COUNT(*) FROM staging_us_confirmed
        WHERE COUNTRY <> 'US'
""")

count_staging_global_confirmed = ("""
        SELECT COUNT(*) FROM staging_global_confirmed
""")

count_fact_metrics = ("""
        SELECT COUNT(*) FROM fact_metrics
""")


def check_counts(cur, conn):
    '''
    analytical queries to make sure row counts are valid
    '''
    cur.execute(count_queries[0])
    us_count = cur.fetchone()[0]
    cur.execute(count_queries[1])
    global_count = cur.fetchone()[0]
    cur.execute(count_queries[2])
    fact_count = cur.fetchone()[0]
    diff = us_count + global_count - fact_count
    print(f'Difference in row count (staging-fact): {diff}')


def check_sum_confirmed(cur, conn):
    cur.execute(sum_confirmed[0])
    us_sum = cur.fetchone()[0]
    cur.execute(sum_confirmed[1])
    global_sum = cur.fetchone()[0]
    cur.execute(sum_confirmed[2])
    fact_sum = cur.fetchone()[0]
    diff = us_sum + global_sum - fact_sum
    print(f'Difference in confirmed sums: {diff}')


def check_sum_deaths(cur, conn):
    cur.execute(sum_deaths[0])
    us_sum = cur.fetchone()[0]
    cur.execute(sum_deaths[1])
    global_sum = cur.fetchone()[0]
    cur.execute(sum_deaths[2])
    fact_sum = cur.fetchone()[0]
    diff = us_sum + global_sum - fact_sum
    print(f'Difference in deaths sums: {diff}')


def check_sum_recovered(cur, conn):
    cur.execute(sum_recovered[0])
    global_sum = cur.fetchone()[0]
    cur.execute(sum_recovered[1])
    fact_sum = cur.fetchone()[0]
    diff = global_sum - fact_sum
    print(f'Difference in recovered sums: {diff}')


def isnull_tables(cur, conn):
    '''
    analytical queries to make sure no null values
    '''
    for table_name, query in isnull_queries.items():
        #print(query)
        cur.execute(query)
        results = cur.fetchone()
        print(f'For table {table_name}, the count of nulls is {results[0]}')


def main():
    '''
    load, insert, and analyze tables
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()
    cur.execute(aws_s3_extension)

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    check_counts(cur, conn)
    check_sum_confirmed(cur, conn)
    check_sum_deaths(cur, conn)
    check_sum_recovered(cur, conn)
    isnull_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
