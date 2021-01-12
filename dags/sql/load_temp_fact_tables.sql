
-- confirmed_temp_insert
INSERT INTO confirmed_temp (location_id, dt, confirmed)
(SELECT location_id, dt, CAST(confirmed as INT) confirmed
FROM staging_global_confirmed
JOIN location
on concat_ws(', ', staging_global_confirmed.state, staging_global_confirmed.country) = location.combined_key)

UNION ALL

(SELECT location_id, dt, confirmed FROM
staging_us_confirmed
JOIN location
on concat_ws(', ', staging_us_confirmed.county, staging_us_confirmed.state, staging_us_confirmed.country) = location.combined_key);

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

-- recovered_temp_insert
INSERT INTO recovered_temp (location_id, dt, recovered)
SELECT location_id, dt, CAST(recovered as INT) recovered
FROM staging_global_recovered
JOIN location
on staging_global_recovered.country <> 'Canada'
and concat_ws(', ', staging_global_recovered.state, staging_global_recovered.country) = location.combined_key;

