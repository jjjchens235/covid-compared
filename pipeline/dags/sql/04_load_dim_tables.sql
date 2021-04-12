-- Insert dim location table
INSERT INTO
  dim.location (
    location_id,
    country,
    state,
    county,
    population,
    combined_key
  )
SELECT
  location_id,
  country,
  state,
  county,
  CAST(population AS INT) population,
  combined_key
FROM
  staging.staging_location ON CONFLICT (location_id) DO NOTHING;

--insert into iso2 table
INSERT INTO
  dim.iso2 (iso2, location_id)
SELECT
  iso2,
  location_id
FROM
  staging.staging_location
WHERE
  iso2 IS NOT NULL ON CONFLICT (iso2_id) DO NOTHING;

--insert into lat_lon table
INSERT INTO
  dim.lat_lon (lat, lon, location_id)
SELECT
  lat,
  lon,
  location_id
FROM
  staging.staging_location ON CONFLICT (lat_lon_id) DO NOTHING;

-- Insert dim time table
INSERT INTO
  dim.time (dt, YEAR, MONTH, DAY, weekday)
SELECT
  DISTINCT dt,
  EXTRACT(
    YEAR
    FROM
      dt
  ) AS YEAR,
  EXTRACT(
    MONTH
    FROM
      dt
  ) AS MONTH,
  EXTRACT(
    DAY
    FROM
      dt
  ) AS DAY,
  EXTRACT(
    dow
    FROM
      dt
  ) AS weekday
FROM
  staging.staging_global_confirmed ON CONFLICT (dt) DO NOTHING;

