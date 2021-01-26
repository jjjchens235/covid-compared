
CREATE SCHEMA staging;
CREATE SCHEMA temp;
CREATE SCHEMA dim;
CREATE SCHEMA fact;

-- Staging tables ----
CREATE TABLE IF NOT EXISTS staging.staging_us_confirmed(
				country varchar,
				state varchar,
				county varchar,
				dt date,
				confirmed float
);


CREATE TABLE IF NOT EXISTS staging.staging_global_confirmed(
				country varchar,
				state varchar,
				dt date,
				confirmed float
);


CREATE TABLE IF NOT EXISTS staging.staging_us_deaths(
				country varchar,
				state varchar,
				county varchar,
				dt date,
				deaths float
);



CREATE TABLE IF NOT EXISTS staging.staging_global_deaths(
				country varchar,
				state varchar,
				dt date,
				deaths float
);

CREATE TABLE IF NOT EXISTS staging.staging_global_recovered(
				country varchar,
				state varchar,
				dt date,
				recovered float
);

CREATE TABLE IF NOT EXISTS staging.staging_location(
				location_id int,
				country varchar,
				state varchar,
				iso2 varchar,
				county varchar,
				population float,
				lat varchar,
				lon varchar,
				combined_key varchar
);
-- ---------------- DiM TABLES -------------

CREATE TABLE IF NOT EXISTS dim.location(
				location_id int PRIMARY KEY,
				country varchar,
				state varchar,
				county varchar,
				population int,
				combined_key varchar
);

CREATE TABLE IF NOT EXISTS dim.iso2(
				iso2_id	SERIAL PRIMARY KEY,
				iso2 varchar,
				location_id int
);

CREATE TABLE IF NOT EXISTS dim.lat_lon(
				lat_lon_id	SERIAL PRIMARY KEY,
				lat varchar,
				lon varchar,
				location_id int
);

CREATE TABLE IF NOT EXISTS dim.time(
				dt date PRIMARY KEY,
				year int,
				month int,
				day int,
				weekday int
);

-- ---------------- TEMP FACT TABLE -------------
-- ----- get foreign keys for our intermediate fact tables

CREATE TABLE IF NOT EXISTS temp.confirmed_temp(
				location_id int,
				dt date,
				confirmed int
);

CREATE TABLE IF NOT EXISTS temp.deaths_temp(
				location_id int,
				dt date,
				deaths int
);

CREATE TABLE IF NOT EXISTS temp.recovered_temp(
				location_id int,
				dt date,
				recovered int
);


-- ---------------- FACT TABLE -------------

CREATE TABLE if NOT EXISTS fact.fact_metrics (
		location_id int,
		dt date,
		confirmed int,
		deaths int,
		recovered int,
		PRIMARY KEY(location_id, dt)
);

CREATE TABLE if NOT EXISTS fact.fact_metrics_moving_avg (
		location_id int,
		dt date,
		confirmed float,
		deaths float,
		recovered float,
		PRIMARY KEY(location_id, dt)
);


