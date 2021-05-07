CREATE SCHEMA IF NOT EXISTS staging;

CREATE SCHEMA IF NOT EXISTS TEMP;

CREATE SCHEMA IF NOT EXISTS dim;

CREATE SCHEMA IF NOT EXISTS fact;

-- Staging tables ----
CREATE TABLE IF NOT EXISTS staging.staging_us_confirmed(
  country VARCHAR,
  state VARCHAR,
  county VARCHAR,
  dt date,
  confirmed FLOAT
);

CREATE TABLE IF NOT EXISTS staging.staging_global_confirmed(
  country VARCHAR,
  state VARCHAR,
  dt date,
  confirmed FLOAT
);

CREATE TABLE IF NOT EXISTS staging.staging_us_deaths(
  country VARCHAR,
  state VARCHAR,
  county VARCHAR,
  dt date,
  deaths FLOAT
);

CREATE TABLE IF NOT EXISTS staging.staging_global_deaths(country VARCHAR, state VARCHAR, dt date, deaths FLOAT);

CREATE TABLE IF NOT EXISTS staging.staging_global_recovered(
  country VARCHAR,
  state VARCHAR,
  dt date,
  recovered FLOAT
);

CREATE TABLE IF NOT EXISTS staging.staging_location(
  location_id INT,
  country VARCHAR,
  state VARCHAR,
  iso2 VARCHAR,
  iso3 VARCHAR,
  county VARCHAR,
  population FLOAT,
  lat VARCHAR,
  lon VARCHAR,
  combined_key VARCHAR
);

-- ---------------- DiM TABLES -------------
CREATE TABLE IF NOT EXISTS dim.location(
  location_id INT PRIMARY KEY,
  country VARCHAR,
  state VARCHAR,
  county VARCHAR,
  population INT,
  combined_key VARCHAR UNIQUE
);

CREATE TABLE IF NOT EXISTS dim.iso(
  iso_id SERIAL PRIMARY KEY,
  iso2 VARCHAR,
  iso3 VARCHAR,
  location_id INT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim.lat_lon(
  lat_lon_id SERIAL PRIMARY KEY,
  lat VARCHAR,
  lon VARCHAR,
  lat_lon VARCHAR,
  location_id INT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim.time(
  dt date PRIMARY KEY,
  YEAR INT,
  MONTH INT,
  DAY INT,
  weekday INT
);

-- ---------------- TEMP FACT TABLE -------------
-- ----- get foreign keys for our intermediate fact tables
CREATE TABLE IF NOT EXISTS TEMP.confirmed_temp(location_id INT, dt date, confirmed INT);

CREATE TABLE IF NOT EXISTS TEMP.deaths_temp(location_id INT, dt date, deaths INT);

CREATE TABLE IF NOT EXISTS TEMP.recovered_temp(location_id INT, dt date, recovered INT);

-- ---------------- FACT TABLE -------------
CREATE TABLE IF NOT EXISTS fact.fact_metrics (
  location_id INT,
  dt date,
  confirmed INT,
  deaths INT,
  recovered INT,
  PRIMARY KEY(location_id, dt)
);

CREATE TABLE IF NOT EXISTS fact.fact_metrics_moving_avg (
  location_id INT,
  dt date,
  confirmed FLOAT,
  deaths FLOAT,
  recovered FLOAT,
  PRIMARY KEY(location_id, dt)
);

