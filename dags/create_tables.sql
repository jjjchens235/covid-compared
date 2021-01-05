

-- Staging tables ----
CREATE TABLE IF NOT EXISTS staging_us_confirmed(
				country varchar,
				iso2 varchar,
				state varchar,
				county varchar,
				population float,
				dt date,
				confirmed float
);


CREATE TABLE IF NOT EXISTS staging_global_confirmed(
				country varchar,
				state varchar,
				population float,
				dt date,
				confirmed float
);


CREATE TABLE IF NOT EXISTS staging_us_deaths(
				country varchar,
				iso2 varchar,
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


-- ---------------- DiM TABLES -------------

CREATE TABLE IF NOT EXISTS location(
				location_id INTEGER GENERATED BY DEFAULT as Identity PRIMARY KEY,
				country varchar,
				state varchar,
				iso2 varchar,
				county varchar,
				population int
				sc_key varchar
);

CREATE TABLE IF NOT EXISTS time(
				dt date PRIMARY KEY,
				year int,
				month int,
				day int,
				weekday int
);

-- ---------------- TEMP FACT TABLES -------------
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


-- ---------------- FACT TABLES -------------

CREATE TABLE if NOT EXISTS fact_metrics (
		location_id int,
		dt date,
		confirmed int,
		deaths int,
		recovered int,
		PRIMARY KEY(location_id, dt)
);

