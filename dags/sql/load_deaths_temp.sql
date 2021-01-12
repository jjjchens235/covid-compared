
-- death_temp_insert
INSERT INTO deaths_temp (location_id, dt, deaths)
(SELECT location_id, dt, CAST(deaths as INT) deaths
FROM staging_global_deaths
JOIN location
on concat_ws(', ', staging_global_deaths.state, staging_global_deaths.country) = location.combined_key)

UNION ALL

(SELECT location_id, dt, deaths FROM
staging_us_deaths
JOIN location
on concat_ws(', ', staging_us_deaths.county, staging_us_deaths.state, staging_us_deaths.country) = location.combined_key);
