
-- confirmed_temp_insert
INSERT INTO confirmed_temp (location_id, dt, confirmed)
(SELECT location_id, dt, CAST(confirmed as INT) confirmed
FROM staging_global_confirmed
JOIN location
on staging_global_confirmed.country <> 'US'
and concat(staging_global_confirmed.state, staging_global_confirmed.country) = location.state_country)

UNION ALL

(SELECT location_id, dt, confirmed FROM
staging_us_confirmed
JOIN location
on concat(staging_us_confirmed.county, staging_us_confirmed.state, staging_us_confirmed.country) = location.county_state_country);
