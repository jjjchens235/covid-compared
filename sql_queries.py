import configparser

# CONFIG
cfg = configparser.ConfigParser()
cfg.read('dwh.cfg')

staging_counties_confirmed_drop = "drop table if exists staging_counties_confirmed"
staging_global_confirmed_drop = "drop table if exists staging_global_confirmed"

staging_us_deaths_drop = "drop table if exists staging_us_deaths"
staging_global_deaths_drop = "drop table if exists staging_global_deaths"

staging_global_recovered_drop = "drop table if exists staging_global_recovered"

dim_countries_drop = "drop table if exists countries"
dim_states_drop = "drop table if exists states"
dim_counties_drop = "drop table if exists counties"
dim_time_drop = "drop table if exists time"

counties_confirmed_temp_drop = "drop table if exists counties_confirmed_temp"
global_confirmed_temp_drop = "drop table if exists global_confirmed_temp"

counties_deaths_temp_drop = "drop table if exists counties_deaths_temp"
global_deaths_temp_drop = "drop table if exists global_deaths_temp"

global_recovered_temp_drop = "drop table if exists global_recovered_temp"


staging_counties_confirmed_create = ("""CREATE TABLE if not exists staging_counties_confirmed(
        country varchar,
        iso2 varchar,
        state varchar,
        county varchar,
        dt date,
        confirmed float
)
""")

staging_counties_confirmed_copy = ("""
        copy staging_counties_confirmed from {data_bucket}
        credentials 'aws_iam_role={role_arn}'
        region 'us-west-2'
        delimiter as '\\t'
        DATEFORMAT 'YYYY-MM-DD'
        EMPTYASNULL
        IGNOREHEADER 1;
    """).format(data_bucket=cfg.get('S3', 'US_CONFIRMED'), role_arn=cfg.get('IAM_ROLE', 'ARN'))


staging_global_confirmed_create = ("""CREATE TABLE if not exists staging_global_confirmed(
        country varchar,
        state varchar,
        dt date,
        confirmed float
)
""")

staging_global_confirmed_copy = ("""
        copy staging_global_confirmed from {data_bucket}
        credentials 'aws_iam_role={role_arn}'
        region 'us-west-2'
        delimiter as '\\t'
        DATEFORMAT 'YYYY-MM-DD'
        EMPTYASNULL
        IGNOREHEADER 1;
    """).format(data_bucket=cfg.get('S3', 'GLOBAL_CONFIRMED'), role_arn=cfg.get('IAM_ROLE', 'ARN'))


staging_us_deaths_create = ("""CREATE TABLE if not exists staging_us_deaths(
        country varchar,
        iso2 varchar,
        state varchar,
        county varchar,
        dt date,
        deaths float
)
""")

staging_us_deaths_copy = ("""
        copy staging_us_deaths from {data_bucket}
        credentials 'aws_iam_role={role_arn}'
        region 'us-west-2'
        delimiter as '\\t'
        DATEFORMAT 'YYYY-MM-DD'
        EMPTYASNULL
        IGNOREHEADER 1;
    """).format(data_bucket=cfg.get('S3', 'US_DEATH'), role_arn=cfg.get('IAM_ROLE', 'ARN'))


staging_global_deaths_create = ("""CREATE TABLE if not exists staging_global_deaths(
        country varchar,
        state varchar,
        dt date,
        deaths float
)
""")

staging_global_deaths_copy = ("""
        copy staging_global_deaths from {data_bucket}
        credentials 'aws_iam_role={role_arn}'
        region 'us-west-2'
        delimiter as '\\t'
        DATEFORMAT 'YYYY-MM-DD'
        EMPTYASNULL
        IGNOREHEADER 1;
    """).format(data_bucket=cfg.get('S3', 'GLOBAL_DEATH'), role_arn=cfg.get('IAM_ROLE', 'ARN'))


staging_global_recovered_create = ("""CREATE TABLE if not exists staging_global_recovered(
        country varchar,
        state varchar,
        dt date,
        recovered float
)
""")

staging_global_recovered_copy = ("""
        copy staging_global_recovered from {data_bucket}
        credentials 'aws_iam_role={role_arn}'
        region 'us-west-2'
        delimiter as '\\t'
        DATEFORMAT 'YYYY-MM-DD'
        EMPTYASNULL
        IGNOREHEADER 1;
    """).format(data_bucket=cfg.get('S3', 'GLOBAL_RECOVERED'), role_arn=cfg.get('IAM_ROLE', 'ARN'))


dim_countries_create = (""" CREATE TABLE if not exists countries(
        country_id INTEGER Identity(0, 1) PRIMARY KEY,
        country varchar
)
""")


dim_countries_insert = ("""
    INSERT INTO countries (country)
    SELECT DISTINCT country
    FROM staging_counties_confirmed

    UNION

    SELECT DISTINCT country
    FROM staging_global_confirmed
""")


dim_states_create = (""" CREATE TABLE if not exists states(
        state_id INTEGER Identity(0, 1) PRIMARY KEY,
        state varchar,
        country_id int,
        iso2 varchar
)
""")

dim_states_insert = ("""
    INSERT INTO states (state, country_id, iso2)
    SELECT distinct state, country_id, iso2 from staging_counties_confirmed
    join countries on staging_counties_confirmed.country = countries.country

    UNION

    SELECT distinct state, country_id, NULL as iso2 from staging_global_confirmed
    join countries on staging_global_confirmed.country = countries.country
    and staging_global_confirmed.state IS NOT NULL
    """)


dim_counties_create = (""" CREATE TABLE if not exists counties(
        county_id INTEGER Identity(0, 1) PRIMARY KEY,
        county varchar,
        iso2 varchar,
        state_id int
)
""")


# don't think I need a country id since it's all USA
dim_counties_insert = ("""
    INSERT INTO counties (county, iso2, state_id)

    SELECT distinct county, staging_counties_confirmed.iso2, state_id from staging_counties_confirmed

    JOIN

    -- can't join on state only b/c Princess state in both US & Canada
    (select c.country, s.state, s.state_id from states s
    join countries c
    on s.country_id = c.country_id) as sc

    on staging_counties_confirmed.state = sc.state
    and staging_counties_confirmed.country = sc.country
    and staging_counties_confirmed.county is NOT NULL
    """)


dim_time_create = (""" CREATE TABLE if not exists time(
        dt date PRIMARY KEY,
        year int,
        month int,
        day int,
        weekday int
)
""")

dim_time_insert = ("""
    INSERT INTO time (dt, year, month, day, weekday)
    SELECT distinct dt, extract(year from dt) as year, extract(month from dt) as month, extract(day from dt) as day, extract(dow from dt) as weekday from staging_global_confirmed
    """)

# --------- temp tables - CONFIRMED ------
# ----- get foreign keys for our intermediate fact tables
counties_confirmed_temp_create = (""" CREATE TABLE if not exists counties_confirmed_temp(
        county_id int,
        state_id int,
        dt date,
        confirmed int
)
""")

counties_confirmed_temp_insert = ("""
    INSERT INTO counties_confirmed_temp (county_id, state_id, dt, confirmed)
    SELECT county_id, state_id, dt, CAST(confirmed as INT) confirmed
    FROM staging_counties_confirmed JOIN
    (select county_id, county, states.state_id, states.state from counties join states on counties.state_id = states.state_id) cs
    on staging_counties_confirmed.county = cs.county
    and staging_counties_confirmed.state = cs.state
""")

global_confirmed_temp_create = (""" CREATE TABLE if not exists global_confirmed_temp(
        country_id int,
        state_id int,
        dt date,
        confirmed int
)
""")

global_confirmed_temp_insert = ("""
    INSERT INTO global_confirmed_temp (country_id, state_id, dt, confirmed)

    -- global data by country/state level, excluding US
    SELECT country_id, state_id, dt, CAST(confirmed as INT) confirmed
    FROM staging_global_confirmed
    LEFT JOIN
    (select c.country_id, s.state_id, c.country, s.state from states s
     right join countries c
     on s.country_id = c.country_id ) as sc
    on staging_global_confirmed.country = sc.country
    and (staging_global_confirmed.state = sc.state or (staging_global_confirmed.state iS NULL and sc.state IS NULL))
    and staging_global_confirmed.country != 'US'

    UNION

    -- get US data by state level
    SELECT sc.country_id, sc.state_id, dt, confirmed from

        -- US confirmed, group counties by state
        ((SELECT country, state, dt, CAST(sum(confirmed) as INT) confirmed
        FROM staging_counties_confirmed
        GROUP BY country, state, dt) as us

        JOIN

        /* Must match on state AND country to get correct state_id
        Else you bring in Princess cruises twice (US & Can)
        */

        (select c.country_id, s.state_id, c.country, s.state from states s
         join countries c
         on s.country_id = c.country_id ) as sc

        on us.state = sc.state and us.country = sc.country)
""")

# --------- temp tables - DEATHS ------
# ----- get foreign keys for our intermediate fact tables
counties_deaths_temp_create = (""" CREATE TABLE if not exists counties_deaths_temp(
        county_id int,
        state_id int,
        dt date,
        deaths int
)
""")

counties_deaths_temp_insert = ("""
    INSERT INTO counties_deaths_temp (county_id, state_id, dt, deaths)
    SELECT county_id, state_id, dt, CAST(deaths as INT) deaths
    FROM staging_us_deaths JOIN
    (select county_id, county, states.state_id, states.state from counties join states on counties.state_id = states.state_id) cs
    on staging_us_deaths.county = cs.county
    and staging_us_deaths.state = cs.state
""")

global_deaths_temp_create = (""" CREATE TABLE if not exists global_deaths_temp(
        country_id int,
        state_id int,
        dt date,
        deaths int
)
""")

global_deaths_temp_insert = ("""
    INSERT INTO global_deaths_temp (country_id, state_id, dt, deaths)

    -- global data by country/state level, excluding US
    SELECT country_id, state_id, dt, CAST(deaths as INT) deaths
    FROM staging_global_deaths
    LEFT JOIN
    (select c.country_id, s.state_id, c.country, s.state from states s
     right join countries c
     on s.country_id = c.country_id ) as sc
    on staging_global_deaths.country = sc.country
    and (staging_global_deaths.state = sc.state or (staging_global_deaths.state iS NULL and sc.state IS NULL))
    and staging_global_deaths.country != 'US'

    UNION

    -- get US data by state level
    SELECT sc.country_id, sc.state_id, dt, deaths from

        -- US deaths, group counties by state
        ((SELECT country, state, dt, CAST(sum(deaths) as INT) deaths
        FROM staging_us_deaths
        GROUP BY country, state, dt) as us

        JOIN

        /* Must match on state AND country to get correct state_id
        Else you bring in Princess cruises twice (US & Can)
        */

        (select c.country_id, s.state_id, c.country, s.state from states s
         join countries c
         on s.country_id = c.country_id ) as sc

        on us.state = sc.state and us.country = sc.country)
""")


global_recovered_temp_create = (""" CREATE TABLE if not exists global_recovered_temp(
        country_id int,
        state_id int,
        dt date,
        recovered int
)
""")


global_recovered_temp_insert = ("""
    INSERT INTO global_recovered_temp (country_id, state_id, dt, recovered)

    -- global data by country/state level, excluding US
    SELECT country_id, state_id, dt, CAST(recovered as INT) recovered
    FROM staging_global_recovered
    LEFT JOIN
    (select c.country_id, s.state_id, c.country, s.state from states s
     right join countries c
     on s.country_id = c.country_id ) as sc
    on staging_global_recovered.country = sc.country
    and (staging_global_recovered.state = sc.state or (staging_global_recovered.state iS NULL and sc.state IS NULL))
    and staging_global_recovered.country != 'US'

""")

fact_counties_create = ("""
    CREATE TABLE if NOT EXISTS fact_counties (
        county_id int,
        dt date,
        confirmed int,
        deaths int,
        recovered int
    )
""")

fact_counties_insert = ("""
        select coalesce(d.county_id, c.county_id) county_id, coalesce(d.dt, c.dt) dt, coalesce(c.confirmed, 0) confirmed, coalesce(d.deaths, 0) deaths
        from counties_deaths_temp d
        full outer join counties_confirmed_temp c on d.county_id = c.county_id and d.dt = c.dt
""")

fact_global_create = ("""
    CREATE TABLE if NOT EXISTS fact_global (
        country_id int,
        state_id int,
        dt date,
        confirmed int,
        deaths int,
        recovered int
    )
""")

fact_global_insert = ("""
        select coalesce(d.country_id, c.country_id, r.country_id) country_id, coalesce(d.state_id, c.state_id, r.state_id) state_id, coalesce(d.dt, c.dt, r.dt) dt, coalesce(c.confirmed, 0) confirmed, coalesce(d.deaths, 0) deaths, coalesce(r.recovered, 0) recovered
        from global_deaths_temp d
        full outer join global_confirmed_temp c on d.country_id = c.country_id and d.state_id = c.state_id and d.dt = c.dt
        full outer join global_recovered_temp r on r.country_id = d.country_id and r.state_id = d.state_id and r.dt = d.dt
""")

base_queries = ['staging_counties_confirmed', 'staging_global_confirmed', 'staging_us_deaths', 'staging_global_deaths', 'staging_global_recovered', 'dim_countries', 'dim_states', 'dim_counties', 'dim_time', 'counties_confirmed_temp', 'global_confirmed_temp', 'counties_deaths_temp', 'global_deaths_temp', 'global_recovered_temp']


create_table_queries = [eval(query + '_create') for query in base_queries]
drop_table_queries = [eval(query + '_drop') for query in base_queries]

copy_table_queries = [eval(query + '_copy') for query in base_queries if 'staging' in query]
insert_table_queries = [eval(query + '_insert') for query in base_queries if 'staging' not in query]

count_queries = []
