

-- Staging tables ----
CREATE TABLE IF NOT EXISTS staging_us_confirmed(
				country varchar,
				state varchar,
				county varchar,
				dt date,
				confirmed float
);


CREATE TABLE IF NOT EXISTS staging_global_confirmed(
				country varchar,
				state varchar,
				dt date,
				confirmed float
);


CREATE TABLE IF NOT EXISTS staging_us_deaths(
				country varchar,
				state varchar,
				county varchar,
				dt date,
				deaths float
);



CREATE TABLE IF NOT EXISTS staging_global_deaths(
				country varchar,
				state varchar,
				dt date,
				deaths float
);

CREATE TABLE IF NOT EXISTS staging_global_recovered(
				country varchar,
				state varchar,
				dt date,
				recovered float
);

CREATE TABLE IF NOT EXISTS staging_location(
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

CREATE TABLE IF NOT EXISTS location(
				location_id int PRIMARY KEY,
				country varchar,
				state varchar,
				county varchar,
				population int,
				combined_key varchar
);

CREATE TABLE IF NOT EXISTS iso2(
				iso2_id	SERIAL PRIMARY KEY,
				iso2 varchar,
				location_id int
);

CREATE TABLE IF NOT EXISTS lat_lon(
				lat_lon_id	SERIAL PRIMARY KEY,
				lat varchar,
				lon varchar,
				location_id int
);

CREATE TABLE IF NOT EXISTS time(
				dt date PRIMARY KEY,
				year int,
				month int,
				day int,
				weekday int
);

-- ---------------- TEMP FACT TABLE -------------
-- ----- get foreign keys for our intermediate fact tables

CREATE TABLE IF NOT EXISTS confirmed_temp(
				location_id int,
				dt date,
				confirmed int
);

CREATE TABLE IF NOT EXISTS deaths_temp(
				location_id int,
				dt date,
				deaths int
);

CREATE TABLE IF NOT EXISTS recovered_temp(
				location_id int,
				dt date,
				recovered int
);


-- ---------------- FACT TABLE -------------

CREATE TABLE if NOT EXISTS fact_metrics (
		location_id int,
		dt date,
		confirmed int,
		deaths int,
		recovered int,
		PRIMARY KEY(location_id, dt)
);

CREATE TABLE if NOT EXISTS fact_metrics_moving_avg (
		location_id int,
		dt date,
		confirmed float,
		deaths float,
		recovered float,
		PRIMARY KEY(location_id, dt)
);


------- BI Tables -----
CREATE TABLE if NOT EXISTS bi_county (
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

CREATE TABLE if NOT EXISTS bi_state (
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

CREATE TABLE if NOT EXISTS bi_country (
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
