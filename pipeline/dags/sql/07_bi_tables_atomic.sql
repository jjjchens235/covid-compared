-- reference: https://dba.stackexchange.com/questions/100779/how-to-atomically-replace-table-data-in-postgresql
------- BI Tables -----
BEGIN;

CREATE SCHEMA IF NOT EXISTS bi;

CREATE TABLE IF NOT EXISTS bi.bi_county_new (
  location_id INT,
  combined_key VARCHAR,
  country VARCHAR,
  state VARCHAR,
  county VARCHAR,
  population INT,
  dt date,
  confirmed INT,
  deaths INT,
  recovered INT,
  confirmed_per_capita NUMERIC,
  deaths_per_capita NUMERIC,
  recovered_per_capita NUMERIC,
  PRIMARY KEY(location_id, dt)
);

CREATE TABLE IF NOT EXISTS bi.bi_state_new (
  location_id INT,
  combined_key VARCHAR,
  country VARCHAR,
  state VARCHAR,
  population INT,
  dt date,
  confirmed INT,
  deaths INT,
  recovered INT,
  confirmed_per_capita NUMERIC,
  deaths_per_capita NUMERIC,
  recovered_per_capita NUMERIC,
  PRIMARY KEY(location_id, dt)
);

CREATE TABLE IF NOT EXISTS bi.bi_country_new (
  location_id INT,
  combined_key VARCHAR,
  population INT,
  dt date,
  confirmed INT,
  deaths INT,
  recovered INT,
  confirmed_per_capita NUMERIC,
  deaths_per_capita NUMERIC,
  recovered_per_capita NUMERIC,
  PRIMARY KEY(location_id, dt)
);

-- insert into bi tables to be used for dash app
-- county
INSERT INTO
  bi.bi_county_new
SELECT
  l.location_id,
  combined_key,
  country,
  state,
  county,
  population,
  dt,
  confirmed,
  deaths,
  recovered,
  round(
    (
      100000.0 / CAST(population AS NUMERIC) * CAST(confirmed AS NUMERIC)
    ),
    2
  ) confirmed_per_capita,
  round(
    (
      100000.0 / CAST(population AS NUMERIC) * CAST(deaths AS NUMERIC)
    ),
    2
  ) deaths_per_capita,
  round(
    (
      100000.0 / CAST(population AS NUMERIC) * CAST(recovered AS NUMERIC)
    ),
    2
  ) recovered_per_capita
FROM
  fact.fact_metrics_moving_avg f
  JOIN dim.location l ON f.location_id = l.location_id
WHERE
  county IS NOT NULL ON CONFLICT (location_id, dt) DO
UPDATE
SET
  confirmed = excluded.confirmed,
  deaths = excluded.deaths,
  recovered = excluded.recovered,
  confirmed_per_capita = excluded.confirmed_per_capita,
  deaths_per_capita = excluded.deaths_per_capita,
  recovered_per_capita = excluded.recovered_per_capita;

-- state
INSERT INTO
  bi.bi_state_new
SELECT
  l.location_id,
  combined_key,
  country,
  state,
  population,
  dt,
  confirmed,
  deaths,
  recovered,
  round(
    (
      100000.0 / CAST(population AS NUMERIC) * CAST(confirmed AS NUMERIC)
    ),
    2
  ) confirmed_per_capita,
  round(
    (
      100000.0 / CAST(population AS NUMERIC) * CAST(deaths AS NUMERIC)
    ),
    2
  ) deaths_per_capita,
  round(
    (
      100000.0 / CAST(population AS NUMERIC) * CAST(recovered AS NUMERIC)
    ),
    2
  ) recovered_per_capita
FROM
  fact.fact_metrics_moving_avg f
  JOIN dim.location l ON f.location_id = l.location_id
WHERE
  state IS NOT NULL
  AND county IS NULL ON CONFLICT (location_id, dt) DO
UPDATE
SET
  confirmed = excluded.confirmed,
  deaths = excluded.deaths,
  recovered = excluded.recovered,
  confirmed_per_capita = excluded.confirmed_per_capita,
  deaths_per_capita = excluded.deaths_per_capita,
  recovered_per_capita = excluded.recovered_per_capita;

-- country
INSERT INTO
  bi.bi_country_new
SELECT
  l.location_id,
  combined_key,
  population,
  dt,
  confirmed,
  deaths,
  recovered,
  round(
    (
      100000.0 / CAST(population AS NUMERIC) * CAST(confirmed AS NUMERIC)
    ),
    2
  ) confirmed_per_capita,
  round(
    (
      100000.0 / CAST(population AS NUMERIC) * CAST(deaths AS NUMERIC)
    ),
    2
  ) deaths_per_capita,
  round(
    (
      100000.0 / CAST(population AS NUMERIC) * CAST(recovered AS NUMERIC)
    ),
    2
  ) recovered_per_capita
FROM
  fact.fact_metrics_moving_avg f
  JOIN dim.location l ON f.location_id = l.location_id
WHERE
  county IS NULL
  AND state IS NULL ON CONFLICT (location_id, dt) DO
UPDATE
SET
  confirmed = excluded.confirmed,
  deaths = excluded.deaths,
  recovered = excluded.recovered,
  confirmed_per_capita = excluded.confirmed_per_capita,
  deaths_per_capita = excluded.deaths_per_capita,
  recovered_per_capita = excluded.recovered_per_capita;

-- Rename new tables
ALTER TABLE
  IF EXISTS bi.bi_county RENAME TO bi_county_old;

ALTER TABLE
  IF EXISTS bi.bi_state RENAME TO bi_state_old;

ALTER TABLE
  IF EXISTS bi.bi_country RENAME TO bi_country_old;

ALTER TABLE
  IF EXISTS bi.bi_county_new RENAME TO bi_county;

ALTER TABLE
  IF EXISTS bi.bi_state_new RENAME TO bi_state;

ALTER TABLE
  IF EXISTS bi.bi_country_new RENAME TO bi_country;

-- Drop old ones
DROP TABLE IF EXISTS bi.bi_county_old;

DROP TABLE IF EXISTS bi.bi_state_old;

DROP TABLE IF EXISTS bi.bi_country_old;

----------------- Top 5 ---------------------------
-- county
CREATE TABLE IF NOT EXISTS bi.bi_county_top_new (LIKE bi.bi_county INCLUDING ALL);

INSERT INTO
  bi.bi_county_top_new
SELECT
  *
FROM
  bi.bi_county
WHERE
  population > '{{ params.county_thresh }}' ON CONFLICT (location_id, dt) DO
UPDATE
SET
  confirmed = excluded.confirmed,
  deaths = excluded.deaths,
  recovered = excluded.recovered,
  confirmed_per_capita = excluded.confirmed_per_capita,
  deaths_per_capita = excluded.deaths_per_capita,
  recovered_per_capita = excluded.recovered_per_capita;

-- state
CREATE TABLE IF NOT EXISTS bi.bi_state_top_new (LIKE bi.bi_state INCLUDING ALL);

INSERT INTO
  bi.bi_state_top_new
SELECT
  *
FROM
  bi.bi_state
WHERE
  population > '{{ params.state_thresh }}' ON CONFLICT (location_id, dt) DO
UPDATE
SET
  confirmed = excluded.confirmed,
  deaths = excluded.deaths,
  recovered = excluded.recovered,
  confirmed_per_capita = excluded.confirmed_per_capita,
  deaths_per_capita = excluded.deaths_per_capita,
  recovered_per_capita = excluded.recovered_per_capita;

-- country
CREATE TABLE IF NOT EXISTS bi.bi_country_top_new (LIKE bi.bi_country INCLUDING ALL);

INSERT INTO
  bi.bi_country_top_new
SELECT
  *
FROM
  bi.bi_country
WHERE
  population > '{{ params.country_thresh }}' ON CONFLICT (location_id, dt) DO
UPDATE
SET
  confirmed = excluded.confirmed,
  deaths = excluded.deaths,
  recovered = excluded.recovered,
  confirmed_per_capita = excluded.confirmed_per_capita,
  deaths_per_capita = excluded.deaths_per_capita,
  recovered_per_capita = excluded.recovered_per_capita;

-- Rename new tables
ALTER TABLE
  IF EXISTS bi.bi_county_top RENAME TO bi_county_top_old;

ALTER TABLE
  IF EXISTS bi.bi_state_top RENAME TO bi_state_top_old;

ALTER TABLE
  IF EXISTS bi.bi_country_top RENAME TO bi_country_top_old;

ALTER TABLE
  IF EXISTS bi.bi_county_top_new RENAME TO bi_county_top;

ALTER TABLE
  IF EXISTS bi.bi_state_top_new RENAME TO bi_state_top;

ALTER TABLE
  IF EXISTS bi.bi_country_top_new RENAME TO bi_country_top;

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

