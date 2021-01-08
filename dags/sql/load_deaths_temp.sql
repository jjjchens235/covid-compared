
-- death_temp_insert
INSERT INTO deaths_temp (location_id, dt, deaths)
(SELECT location_id, dt, CAST(deaths as INT) deaths
FROM staging_global_deaths
JOIN location
on staging_global_deaths.country <> 'US'
and concat(staging_global_deaths.state, staging_global_deaths.country) = location.state_country)

UNION ALL

(SELECT location_id, dt, deaths FROM
staging_us_deaths
JOIN location
on concat(staging_us_deaths.county, staging_us_deaths.state, staging_us_deaths.country) = location.county_state_country);
