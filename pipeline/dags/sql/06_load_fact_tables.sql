
-- fact_metrics_insert
INSERT INTO fact.fact_metrics (location_id, dt, confirmed, deaths, recovered)
SELECT COALESCE(d.location_id, c.location_id, r.location_id) location_id, COALESCE(d.dt, c.dt, r.dt) dt, COALESCE(c.confirmed, 0) confirmed, COALESCE(d.deaths, 0) deaths, COALESCE(r.recovered, 0) recovered
FROM temp.confirmed_temp c
FULL OUTER JOIN temp.deaths_temp d on d.location_id = c.location_id and d.dt = c.dt
FULL OUTER JOIN temp.recovered_temp r on r.location_id = c.location_id and r.dt = c.dt;


INSERT INTO fact.fact_metrics_moving_avg (location_id, dt, confirmed, deaths, recovered)
SELECT * FROM 
		(SELECT location_id, dt,
		CASE WHEN COUNT(*) OVER(PARTITION BY location_id ORDER BY dt ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING) > 6
		THEN AVG(confirmed) OVER (PARTITION BY location_id ORDER BY dt ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING)::FLOAT
		ELSE NULL
		END AS confirmed,

		CASE WHEN COUNT(*) OVER(PARTITION BY location_id ORDER BY dt ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING) > 6
		THEN AVG(deaths) OVER (PARTITION BY location_id ORDER BY dt ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING)::FLOAT
		ELSE NULL
		END AS deaths,

		CASE WHEN COUNT(*) OVER(PARTITION BY location_id ORDER BY dt ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING) > 6
		THEN AVG(recovered) OVER (PARTITION BY location_id ORDER BY dt ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING)::FLOAT
		ELSE NULL
		END AS recovered

		FROM fact.fact_metrics) tmp
WHERE confirmed IS NOT NULL
