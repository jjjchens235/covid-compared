
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


-- deaths_temp_insert
INSERT INTO deaths_temp (location_id, dt, deaths)
(SELECT location_id, dt, CAST(deaths as INT) deaths
FROM staging_global_deaths
JOIN location
on staging_global_deaths.country <> 'US'
and ((staging_global_deaths.country || staging_global_deaths.state = location.country || location.state) or (staging_global_deaths.state IS NULL and location.state IS NULL and staging_global_deaths.country =  location.country)))

UNION

(SELECT location_id, dt, deaths FROM
staging_us_deaths
JOIN location
on (staging_us_deaths.county || staging_us_deaths.state = location.county || location.state) or (staging_us_deaths.county IS NULL and staging_us_deaths.country || staging_us_deaths.state = location.country || location.state)
);


-- recovered_temp_insert
INSERT INTO recovered_temp (location_id, dt, recovered)
SELECT location_id, dt, CAST(recovered as INT) recovered
FROM staging_global_recovered
JOIN location
on staging_global_recovered.country <> 'US'
and ((staging_global_recovered.country || staging_global_recovered.state = location.country || location.state) or (staging_global_recovered.state IS NULL and location.state IS NULL and staging_global_recovered.country =  location.country));
