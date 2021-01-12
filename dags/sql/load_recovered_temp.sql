
-- recovered_temp_insert
INSERT INTO recovered_temp (location_id, dt, recovered)
SELECT location_id, dt, CAST(recovered as INT) recovered
FROM staging_global_recovered
JOIN location
on staging_global_recovered.country <> 'Canada'
and concat_ws(', ', staging_global_recovered.state, staging_global_recovered.country) = location.combined_key

