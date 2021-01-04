import configparser

# CONFIG
cfg = configparser.ConfigParser()
cfg.read('dwh.cfg')

# ---------------- DROP TABLES -------------
staging_us_confirmed_drop = "drop table if exists staging_us_confirmed"
staging_global_confirmed_drop = "drop table if exists staging_global_confirmed"

staging_us_deaths_drop = "drop table if exists staging_us_deaths"
staging_global_deaths_drop = "drop table if exists staging_global_deaths"

staging_global_recovered_drop = "drop table if exists staging_global_recovered"

dim_location_drop = "drop table if exists location"
dim_time_drop = "drop table if exists time"

confirmed_temp_drop = "drop table if exists confirmed_temp"
deaths_temp_drop = "drop table if exists deaths_temp"
recovered_temp_drop = "drop table if exists recovered_temp"

fact_metrics_drop = "drop table if exists fact_metrics"
#fact_global_drop = "drop table if exists fact_global"

# ---------------- STAGING TABLES -------------

staging_us_confirmed_create = ("""CREATE TABLE IF NOT EXISTS staging_us_confirmed(
        country varchar,
        iso2 varchar,
        state varchar,
        county varchar,
        population float,
        dt date,
        confirmed float
)
""")

staging_us_confirmed_copy = ("""
        copy staging_us_confirmed FROM {data_bucket}
        credentials 'aws_iam_role={role_arn}'
        region 'us-west-2'
        delimiter as '\\t'
        DATEFORMAT 'YYYY-MM-DD'
        EMPTYASNULL
        IGNOREHEADER 1;
    """).format(data_bucket=cfg.get('S3', 'US_CONFIRMED'), role_arn=cfg.get('IAM_ROLE', 'ARN'))


staging_global_confirmed_create = ("""CREATE TABLE IF NOT EXISTS staging_global_confirmed(
        country varchar,
        state varchar,
        population float,
        dt date,
        confirmed float
)
""")

staging_global_confirmed_copy = ("""
        copy staging_global_confirmed FROM {data_bucket}
        credentials 'aws_iam_role={role_arn}'
        region 'us-west-2'
        delimiter as '\\t'
        DATEFORMAT 'YYYY-MM-DD'
        EMPTYASNULL
        IGNOREHEADER 1;
    """).format(data_bucket=cfg.get('S3', 'GLOBAL_CONFIRMED'), role_arn=cfg.get('IAM_ROLE', 'ARN'))


staging_us_deaths_create = ("""CREATE TABLE IF NOT EXISTS staging_us_deaths(
        country varchar,
        iso2 varchar,
        state varchar,
        county varchar,
        dt date,
        deaths float
)
""")

staging_us_deaths_copy = ("""
        copy staging_us_deaths FROM {data_bucket}
        credentials 'aws_iam_role={role_arn}'
        region 'us-west-2'
        delimiter as '\\t'
        DATEFORMAT 'YYYY-MM-DD'
        EMPTYASNULL
        IGNOREHEADER 1;
    """).format(data_bucket=cfg.get('S3', 'US_DEATH'), role_arn=cfg.get('IAM_ROLE', 'ARN'))


staging_global_deaths_create = ("""CREATE TABLE IF NOT EXISTS staging_global_deaths(
        country varchar,
        state varchar,
        dt date,
        deaths float
)
""")

staging_global_deaths_copy = ("""
        copy staging_global_deaths FROM {data_bucket}
        credentials 'aws_iam_role={role_arn}'
        region 'us-west-2'
        delimiter as '\\t'
        DATEFORMAT 'YYYY-MM-DD'
        EMPTYASNULL
        IGNOREHEADER 1;
    """).format(data_bucket=cfg.get('S3', 'GLOBAL_DEATH'), role_arn=cfg.get('IAM_ROLE', 'ARN'))


staging_global_recovered_create = ("""CREATE TABLE IF NOT EXISTS staging_global_recovered(
        country varchar,
        state varchar,
        dt date,
        recovered float
)
""")

staging_global_recovered_copy = ("""
        copy staging_global_recovered FROM {data_bucket}
        credentials 'aws_iam_role={role_arn}'
        region 'us-west-2'
        delimiter as '\\t'
        DATEFORMAT 'YYYY-MM-DD'
        EMPTYASNULL
        IGNOREHEADER 1;
    """).format(data_bucket=cfg.get('S3', 'GLOBAL_RECOVERED'), role_arn=cfg.get('IAM_ROLE', 'ARN'))

# ---------------- DiM TABLES -------------

dim_location_create = (""" CREATE TABLE IF NOT EXISTS location(
        location_id INTEGER Identity(0, 1) PRIMARY KEY,
        country varchar,
        state varchar,
        iso2 varchar,
        county varchar,
        population int
)
""")

dim_location_insert = ("""
    INSERT INTO location (country, state, iso2, county, population)
    SELECT DISTINCT country, state, iso2, county, CAST(population as INT) population
    FROM staging_us_confirmed

    UNION

    SELECT DISTINCT country, state, NULL as iso2, NULL as county, population
    FROM staging_global_confirmed
""")

dim_time_create = (""" CREATE TABLE IF NOT EXISTS time(
        dt date PRIMARY KEY,
        year int,
        month int,
        day int,
        weekday int
)
""")

dim_time_insert = ("""
    INSERT INTO time (dt, year, month, day, weekday)
    SELECT distinct dt, extract(year FROM dt) as year, extract(month FROM dt) as month, extract(day FROM dt) as day, extract(dow FROM dt) as weekday FROM staging_global_confirmed
    """)

# ---------------- TEMP FACT TABLES -------------
# ----- get foreign keys for our intermediate fact tables

confirmed_temp_create = (""" CREATE TABLE IF NOT EXISTS confirmed_temp(
        location_id int,
        dt date,
        confirmed int
)
""")

confirmed_temp_insert = ("""
    INSERT INTO confirmed_temp (location_id, dt, confirmed)
    (SELECT location_id, dt, CAST(confirmed as INT) confirmed
    FROM staging_global_confirmed
    JOIN location
    on staging_global_confirmed.country <> 'US'
    and ((staging_global_confirmed.country || staging_global_confirmed.state = location.country || location.state) or (staging_global_confirmed.state IS NULL and location.state IS NULL and staging_global_confirmed.country =  location.country)))

    UNION

    (SELECT location_id, dt, confirmed FROM
    staging_us_confirmed
    JOIN location
    on (staging_us_confirmed.county || staging_us_confirmed.state = location.county || location.state) or (staging_us_confirmed.county IS NULL and staging_us_confirmed.country || staging_us_confirmed.state = location.country || location.state)
    )
""")

deaths_temp_create = (""" CREATE TABLE IF NOT EXISTS deaths_temp(
        location_id int,
        dt date,
        deaths int
)
""")

deaths_temp_insert = ("""
    INSERT INTO deaths_temp (location_id, dt, deaths)
    (SELECT location_id, dt, CAST(deaths as INT) deaths
    FROM staging_global_deaths
    JOIN location
    on staging_global_deaths.country <> 'US'
    and ((staging_global_deaths.country || staging_global_deaths.state = location.country || location.state) or (staging_global_deaths.state IS NULL and location.state IS NULL and staging_global_deaths.country =  location.country)))

    UNION

    (SELECT location_id, dt, deaths FROM
    staging_us_deaths
    JOIN location
    on (staging_us_deaths.county || staging_us_deaths.state = location.county || location.state) or (staging_us_deaths.county IS NULL and staging_us_deaths.country || staging_us_deaths.state = location.country || location.state)
    )
""")

recovered_temp_create = (""" CREATE TABLE IF NOT EXISTS recovered_temp(
        location_id int,
        dt date,
        recovered int
)
""")

recovered_temp_insert = ("""
    INSERT INTO recovered_temp (location_id, dt, recovered)
    SELECT location_id, dt, CAST(recovered as INT) recovered
    FROM staging_global_recovered
    JOIN location
    on staging_global_recovered.country <> 'US'
    and ((staging_global_recovered.country || staging_global_recovered.state = location.country || location.state) or (staging_global_recovered.state IS NULL and location.state IS NULL and staging_global_recovered.country =  location.country))
""")

# ---------------- FACT TABLES -------------

fact_metrics_create = ("""
    CREATE TABLE if NOT EXISTS fact_metrics (
        location_id int,
        dt date,
        confirmed int,
        deaths int,
        recovered int,
        PRIMARY KEY(location_id, dt)
    )
""")

# creates the first fact table show US us by confirmed/deaths
fact_metrics_insert = ("""
        INSERT INTO fact_metrics (location_id, dt, confirmed, deaths, recovered)
        SELECT COALESCE(d.location_id, c.location_id, r.location_id) location_id, COALESCE(d.dt, c.dt, r.dt) dt, COALESCE(c.confirmed, 0) confirmed, COALESCE(d.deaths, 0) deaths, COALESCE(r.recovered, 0) recovered
        FROM confirmed_temp c
        FULL OUTER JOIN deaths_temp d on d.location_id = c.location_id and d.dt = c.dt
        FULL OUTER JOIN recovered_temp r on r.location_id = c.location_id and r.dt = c.dt
""")


# ---------------- COUNT TABLES -------------
count_staging_us_confirmed = ("""
        SELECT COUNT(*) FROM staging_us_confirmed
""")

count_staging_global_confirmed = ("""
        SELECT COUNT(*) FROM staging_global_confirmed
        WHERE COUNTRY <> 'US'
""")

count_fact_metrics = ("""
        SELECT COUNT(*) FROM fact_metrics
""")


# ---------------- CHECK SUM CONFIRMED -------------
sum_staging_us_confirmed = ("""
        SELECT SUM(confirmed) FROM staging_us_confirmed
""")

sum_staging_global_confirmed = ("""
        SELECT SUM(confirmed) FROM staging_global_confirmed
        WHERE COUNTRY <> 'US'
""")

sum_fact_confirmed = ("""
        SELECT SUM(confirmed) FROM fact_metrics
""")

# ---------------- CHECK SUM DEATHS -------------
sum_staging_us_deaths = ("""
        SELECT SUM(deaths) FROM staging_us_deaths
""")

sum_staging_global_deaths = ("""
        SELECT SUM(deaths) FROM staging_global_deaths
        WHERE COUNTRY <> 'US'
""")

sum_fact_deaths = ("""
        SELECT SUM(deaths) FROM fact_metrics
""")


# ---------------- CHECK SUM RECOVERED -------------

sum_staging_global_recovered = ("""
        SELECT SUM(recovered) FROM staging_global_recovered
        WHERE COUNTRY NOT IN ('US', 'Canada')
""")

sum_fact_recovered = ("""
        SELECT SUM(recovered) FROM fact_metrics
""")


# ---------------- CHECK NULL TABLES -------------
isnull_fact_global = ("""
        SELECT COUNT(*) FROM fact_global WHERE confirmed IS NULL or deaths iS NULL or recovered IS NULL
""")

isnull_fact_metrics = ("""
        SELECT COUNT(*) FROM fact_metrics WHERE confirmed IS NULL or deaths iS NULL
""")

# ---------------- LIST OF QUERIES -------------
base_queries = ['staging_us_confirmed', 'staging_global_confirmed', 'staging_us_deaths', 'staging_global_deaths', 'staging_global_recovered', 'dim_location', 'dim_time', 'confirmed_temp', 'deaths_temp', 'recovered_temp', 'fact_metrics']

#'us_confirmed_temp', 'confirmed_temp', 'us_deaths_temp', 'global_deaths_temp', 'global_recovered_temp', 'fact_metrics', 'temp_fact_global', 'fact_global']


create_table_queries = [eval(query + '_create') for query in base_queries]
drop_table_queries = [eval(query + '_drop') for query in base_queries]

copy_table_queries = [eval(query + '_copy') for query in base_queries if 'staging' in query]
insert_table_queries = [eval(query + '_insert') for query in base_queries if 'staging' not in query]

count_queries = [count_staging_us_confirmed, count_staging_global_confirmed, count_fact_metrics]
sum_confirmed = [sum_staging_us_confirmed, sum_staging_global_confirmed, sum_fact_confirmed]
sum_deaths = [sum_staging_us_deaths, sum_staging_global_deaths, sum_fact_deaths]
sum_recovered = [sum_staging_global_recovered, sum_fact_recovered]

isnull_queries = {'fact_metrics': isnull_fact_metrics}
