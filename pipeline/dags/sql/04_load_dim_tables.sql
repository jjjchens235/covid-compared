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
  -- check combined key because we create our own location_id's for missing locations and this can lead to dup locations, i.e. Yakutat, Alaska, United States
  staging.staging_location ON CONFLICT (combined_key) DO NOTHING;

--insert into iso2 table
INSERT INTO
  dim.iso (iso2, iso3, location_id)
SELECT
  iso2,
	iso3,
  location_id
FROM
  staging.staging_location
WHERE
  iso2 IS NOT NULL ON CONFLICT (location_id) DO NOTHING;

--insert into lat_lon table
INSERT INTO
  dim.lat_lon (lat, lon, lat_lon, location_id)
SELECT
  lat,
  lon,
	concat_ws(', ', lat, lon) lat_lon,
  location_id
FROM
  staging.staging_location
WHERE
  lat IS NOT NULL 
	AND lon IS NOT NULL 
	ON CONFLICT (location_id) DO NOTHING;

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
