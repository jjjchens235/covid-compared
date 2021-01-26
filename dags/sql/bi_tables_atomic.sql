
-- https://dba.stackexchange.com/questions/100779/how-to-atomically-replace-table-data-in-postgresql
------- BI Tables -----
BEGIN;
	
CREATE SCHEMA bi;

CREATE TABLE if NOT EXISTS bi.bi_county_new (
	combined_key varchar,
	country varchar,
	state varchar,
	county varchar,
	population int,
	dt date,
	confirmed int,
	deaths int,
	recovered int,
	confirmed_per_capita numeric,
	deaths_per_capita numeric,
	recovered_per_capita numeric,
	PRIMARY KEY(combined_key, dt)
);

CREATE TABLE if NOT EXISTS bi.bi_state_new (
	combined_key varchar,
	country varchar,
	state varchar,
	population int,
	dt date,
	confirmed int,
	deaths int,
	recovered int,
	confirmed_per_capita numeric,
	deaths_per_capita numeric,
	recovered_per_capita numeric,
	PRIMARY KEY(combined_key, dt)
);

CREATE TABLE if NOT EXISTS bi.bi_country_new (
	combined_key varchar,
	population int,
	dt date,
	confirmed int,
	deaths int,
	recovered int,
	confirmed_per_capita numeric,
	deaths_per_capita numeric,
	recovered_per_capita numeric,
	PRIMARY KEY(combined_key, dt)
);


-- insert into bi tables to be used for dash app

-- county
INSERT INTO bi_county_new (combined_key, country, state, county, population, dt, confirmed, deaths, recovered, confirmed_per_capita, deaths_per_capita, recovered_per_capita)
SELECT combined_key, country, state, county, population, dt, confirmed, deaths, recovered, 

round((100000.0 / cast(population as numeric) * cast(confirmed as numeric)), 2) confirmed_per_capita,
round((100000.0 / cast(population as numeric) * cast(deaths as numeric)), 2) deaths_per_capita,
round((100000.0 / cast(population as numeric) * cast(recovered as numeric)), 2) recovered_per_capita 

FROM fact_metrics_moving_avg f JOIN location l on f.location_id = l.location_id WHERE county IS NOT NULL; 

-- state
INSERT INTO bi_state_new (combined_key, country, state, population, dt, confirmed, deaths, recovered, confirmed_per_capita, deaths_per_capita, recovered_per_capita)
-- need to group by since US states are by county
SELECT combined_key, country, state, population, dt, confirmed, deaths, recovered, 

round((100000.0 / cast(population as numeric) * cast(confirmed as numeric)), 2) confirmed_per_capita,
round((100000.0 / cast(population as numeric) * cast(deaths as numeric)), 2) deaths_per_capita,
round((100000.0 / cast(population as numeric) * cast(recovered as numeric)), 2) recovered_per_capita 

FROM fact_metrics_moving_avg f JOIN location l on f.location_id = l.location_id WHERE state IS NOT NULL and county IS NULL;

-- country
INSERT INTO bi_country_new (combined_key, population, dt, confirmed, deaths, recovered, confirmed_per_capita, deaths_per_capita, recovered_per_capita)
-- need to group by since US states are by county
SELECT combined_key, population, dt, confirmed, deaths, recovered,

round((100000.0 / cast(population as numeric) * cast(confirmed as numeric)), 2) confirmed_per_capita,
round((100000.0 / cast(population as numeric) * cast(deaths as numeric)), 2) deaths_per_capita,
round((100000.0 / cast(population as numeric) * cast(recovered as numeric)), 2) recovered_per_capita 

FROM fact_metrics_moving_avg f JOIN location l on f.location_id = l.location_id WHERE county IS NULL and state IS NULL;


-- ALTER TABLE
ALTER TABLE IF EXISTS bi_county RENAME TO bi_county_old;
ALTER TABLE IF EXISTS bi_state RENAME TO bi_state_old;
ALTER TABLE IF EXISTS bi_country RENAME TO bi_country_old;

ALTER TABLE IF EXISTS bi_county_new RENAME TO bi_county;
ALTER TABLE IF EXISTS bi_state_new RENAME TO bi_state;
ALTER TABLE IF EXISTS bi_country_new RENAME TO bi_country;

DROP TABLE IF EXISTS bi_county_old;
DROP TABLE IF EXISTS bi_state_old;
DROP TABLE IF EXISTS bi_country_old;

COMMIT;
