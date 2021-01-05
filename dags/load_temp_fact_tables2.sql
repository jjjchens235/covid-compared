
-- confirmed_temp_insert
INSERT INTO confirmed_temp (location_id, dt, confirmed)
(SELECT location_id, dt, CAST(confirmed as INT) confirmed
FROM staging_global_confirmed
JOIN location
on staging_global_confirmed.country <> 'US'
and ((staging_global_confirmed.country || staging_global_confirmed.state = location.country || location.state) or (staging_global_confirmed.state IS NULL and location.state IS NULL and staging_global_confirmed.country =  location.country)))

UNION

(SELECT location_id, dt, confirmed FROM
staging_us_confirmed
JOIN location
on (staging_us_confirmed.county || staging_us_confirmed.state = location.county || location.state) or (staging_us_confirmed.county IS NULL and staging_us_confirmed.country || staging_us_confirmed.state = location.country || location.state)
);
