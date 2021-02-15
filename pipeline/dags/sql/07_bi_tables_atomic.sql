
-- reference: https://dba.stackexchange.com/questions/100779/how-to-atomically-replace-table-data-in-postgresql

------- BI Tables -----
BEGIN;
	
CREATE SCHEMA IF NOT EXISTS bi;

CREATE TABLE if NOT EXISTS bi.bi_county_new (
	location_id int,
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
	PRIMARY KEY(location_id, dt)
);

CREATE TABLE if NOT EXISTS bi.bi_state_new (
	location_id int,
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
	PRIMARY KEY(location_id, dt)
);

CREATE TABLE if NOT EXISTS bi.bi_country_new (
	location_id int,
	combined_key varchar,
	population int,
	dt date,
	confirmed int,
	deaths int,
	recovered int,
	confirmed_per_capita numeric,
	deaths_per_capita numeric,
	recovered_per_capita numeric,
	PRIMARY KEY(location_id, dt)
);


-- insert into bi tables to be used for dash app
 
 -- county

INSERT INTO bi.bi_county_new (location_id, combined_key, country, state, county, population, dt, confirmed, deaths, recovered, confirmed_per_capita, deaths_per_capita, recovered_per_capita)
SELECT l.location_id,
			 combined_key,
			 country,
			 state,
			 county,
			 population,
			 dt,
			 confirmed,
			 deaths,
			 recovered,
			 round((100000.0 / cast(population AS numeric) * cast(confirmed AS numeric)), 2) confirmed_per_capita,
			 round((100000.0 / cast(population AS numeric) * cast(deaths AS numeric)), 2) deaths_per_capita,
			 round((100000.0 / cast(population AS numeric) * cast(recovered AS numeric)), 2) recovered_per_capita
FROM fact.fact_metrics_moving_avg f
JOIN dim.location l
	ON f.location_id = l.location_id
	WHERE county IS NOT NULL; -- state
	
 -- state

INSERT INTO bi.bi_state_new (location_id, combined_key, country, state, population, dt, confirmed, deaths, recovered, confirmed_per_capita, deaths_per_capita, recovered_per_capita) -- need to group by since US states are by county

	SELECT l.location_id, 
				 combined_key,
				 country,
				 state,
				 population,
				 dt,
				 confirmed,
				 deaths,
				 recovered,
				 round((100000.0 / cast(population AS numeric) * cast(confirmed AS numeric)), 2) confirmed_per_capita,
				 round((100000.0 / cast(population AS numeric) * cast(deaths AS numeric)), 2) deaths_per_capita,
				 round((100000.0 / cast(population AS numeric) * cast(recovered AS numeric)), 2) recovered_per_capita
FROM fact.fact_metrics_moving_avg f
JOIN dim.location l
	ON f.location_id = l.location_id
	WHERE state IS NOT NULL
		AND county IS NULL;

-- country

INSERT INTO bi.bi_country_new (location_id, combined_key, population, dt, confirmed, deaths, recovered, confirmed_per_capita, deaths_per_capita, recovered_per_capita) -- need to group by since US states are by county

SELECT l.location_id,
				 combined_key,
				 population,
				 dt,
				 confirmed,
				 deaths,
				 recovered,
				 round((100000.0 / cast(population AS numeric) * cast(confirmed AS numeric)), 2) confirmed_per_capita,
				 round((100000.0 / cast(population AS numeric) * cast(deaths AS numeric)), 2) deaths_per_capita,
				 round((100000.0 / cast(population AS numeric) * cast(recovered AS numeric)), 2) recovered_per_capita
FROM fact.fact_metrics_moving_avg f
JOIN dim.location l
	ON f.location_id = l.location_id
	WHERE county IS NULL
		AND state IS NULL;

-- Rename new tables

ALTER TABLE IF EXISTS bi.bi_county RENAME TO bi_county_old;


ALTER TABLE IF EXISTS bi.bi_state RENAME TO bi_state_old;


ALTER TABLE IF EXISTS bi.bi_country RENAME TO bi_country_old;


ALTER TABLE IF EXISTS bi.bi_county_new RENAME TO bi_county;


ALTER TABLE IF EXISTS bi.bi_state_new RENAME TO bi_state;


ALTER TABLE IF EXISTS bi.bi_country_new RENAME TO bi_country;

-- Drop old ones

DROP TABLE IF EXISTS bi.bi_county_old;


DROP TABLE IF EXISTS bi.bi_state_old;


DROP TABLE IF EXISTS bi.bi_country_old;


----------------- Top 5 ---------------------------

 -- county

CREATE TABLE IF NOT EXISTS bi.bi_county_top_new AS
SELECT *
FROM bi.bi_county
WHERE population > '{{ params.county_thresh }}' ;
	
 -- state

CREATE TABLE IF NOT EXISTS bi.bi_state_top_new AS
SELECT *
FROM bi.bi_state
WHERE population > '{{ params.state_thresh }}' ;

-- country
CREATE TABLE IF NOT EXISTS bi.bi_country_top_new AS
SELECT *
FROM bi.bi_country
WHERE population > '{{ params.country_thresh }}' ;

-- Rename new tables

ALTER TABLE IF EXISTS bi.bi_county_top RENAME TO bi_county_top_old;


ALTER TABLE IF EXISTS bi.bi_state_top RENAME TO bi_state_top_old;


ALTER TABLE IF EXISTS bi.bi_country_top RENAME TO bi_country_top_old;


ALTER TABLE IF EXISTS bi.bi_county_top_new RENAME TO bi_county_top;


ALTER TABLE IF EXISTS bi.bi_state_top_new RENAME TO bi_state_top;


ALTER TABLE IF EXISTS bi.bi_country_top_new RENAME TO bi_country_top;

-- Drop old ones

DROP TABLE IF EXISTS bi.bi_county_top_old;
DROP TABLE IF EXISTS bi.bi_state_top_old;
DROP TABLE IF EXISTS bi.bi_country_top_old;

-- Create indexes
CREATE INDEX combined_key_county ON bi.bi_county (combined_key);
CREATE INDEX combined_key_state ON bi.bi_state (combined_key);
CREATE INDEX combined_key_country ON bi.bi_country (combined_key);

CREATE INDEX combined_key_county_top ON bi.bi_county_top (combined_key);
CREATE INDEX combined_key_state_top ON bi.bi_state_top (combined_key);
CREATE INDEX combined_key_country_top ON bi.bi_country_top (combined_key);

COMMIT;

