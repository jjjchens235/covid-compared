
-- fact_metrics_insert
INSERT INTO fact_metrics (location_id, dt, confirmed, deaths, recovered)
SELECT COALESCE(d.location_id, c.location_id, r.location_id) location_id, COALESCE(d.dt, c.dt, r.dt) dt, COALESCE(c.confirmed, 0) confirmed, COALESCE(d.deaths, 0) deaths, COALESCE(r.recovered, 0) recovered
FROM confirmed_temp c
FULL OUTER JOIN deaths_temp d on d.location_id = c.location_id and d.dt = c.dt
FULL OUTER JOIN recovered_temp r on r.location_id = c.location_id and r.dt = c.dt;
