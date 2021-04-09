import psycopg2
from airflow.hooks.base_hook import BaseHook


def validate_fact_metric(upstream_tables, fact_table, metric):
    """
    Validates the chosen metric (confirmed, deaths, or recovered) by comparing the fact table metric against the upstream staging tables, to ensure the sum of the metric has not changed downstream

    upstream_tables -- The US and global staging table for the chosen metric.

    fact_table -- The final derived fact table.

    metric -- The metric to compare.
    """
    rds = BaseHook.get_connection('rds')
    with psycopg2.connect(f"host={rds.host} dbname={rds.schema} user={rds.login} password={rds.password} port={rds.port}") as conn:
        cur = conn.cursor()
        upstream_sum = 0
        # get the sum of the upstream staging tables
        for upstream_table in upstream_tables:
            query = f"SELECT SUM({metric}) FROM {upstream_table}"
            cur.execute(query)
            upstream_sum += cur.fetchone()[0]

        # get the sum of the fact table
        query = f"SELECT SUM({metric}) FROM {fact_table}"
        cur.execute(query)
        fact_sum = cur.fetchone()[0]

    diff = abs(upstream_sum - fact_sum)
    print(f"For {metric} metric...\nupstream sum: {upstream_sum}\nfact_sum: {fact_sum}\nDiff: {diff}")
    if diff > 5:
        raise ValueError("Diff exceeeded threshhold")


def validate_bi_counts():
    """
    Check that each bi table (county, state, country) has a distinct count that matches the upstream fact table
    """

    fact_county_q = 'SELECT COUNT(DISTINCT(combined_key)) FROM fact.fact_metrics f JOIN dim.location l on f.location_id = l.location_id WHERE county IS NOT NULL'
    fact_state_q = 'SELECT COUNT(DISTINCT(combined_key)) FROM fact.fact_metrics f JOIN dim.location l on f.location_id = l.location_id WHERE state IS NOT NULL and county is null'
    fact_country_q = 'SELECT COUNT(DISTINCT(combined_key)) FROM fact.fact_metrics f JOIN dim.location l on f.location_id = l.location_id WHERE state is null and county is null'
    bi_county_q = 'SELECT COUNT(DISTINCT combined_key) FROM bi.bi_county'
    bi_state_q = 'SELECT COUNT(DISTINCT combined_key) FROM bi.bi_state'
    bi_country_q = 'SELECT COUNT(DISTINCT combined_key) FROM bi.bi_country'

    def get_diff(bi_table, fact_q, bi_q):
        rds = BaseHook.get_connection('rds')
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


