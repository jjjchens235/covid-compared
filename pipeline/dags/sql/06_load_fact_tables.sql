-- fact_metrics insert
INSERT INTO fact.fact_metrics (
	location_id, dt, confirmed, deaths, 
	recovered
) 
SELECT 
	Coalesce(
		d.location_id, c.location_id, r.location_id
	) location_id, 
	Coalesce(d.dt, c.dt, r.dt) dt, 
	Coalesce(c.confirmed, 0) confirmed, 
	Coalesce(d.deaths, 0) deaths, 
	Coalesce(r.recovered, 0) recovered 
FROM 
	temp.confirmed_temp c full 
	outer join temp.deaths_temp d ON d.location_id = c.location_id 
	AND d.dt = c.dt full 
	outer join temp.recovered_temp r ON r.location_id = c.location_id 
	AND r.dt = c.dt;
	
-- fact_metrics_moving_avg insert
INSERT INTO fact.fact_metrics_moving_avg (
	location_id, dt, confirmed, deaths, 
	recovered
) 
SELECT 
	* 
FROM 
	(
		SELECT 
			location_id, 
			dt, 
			CASE WHEN Count(*) over(
				PARTITION BY location_id 
				ORDER BY 
					dt ROWS BETWEEN 6 preceding 
					AND 0 following
			) > 6 THEN Avg(confirmed) over (
				PARTITION BY location_id 
				ORDER BY 
					dt ROWS BETWEEN 6 preceding 
					AND 0 following
			) :: FLOAT ELSE NULL END AS confirmed, 
			CASE WHEN Count(*) over(
				PARTITION BY location_id 
				ORDER BY 
					dt ROWS BETWEEN 6 preceding 
					AND 0 following
			) > 6 THEN Avg(deaths) over (
				PARTITION BY location_id 
				ORDER BY 
					dt ROWS BETWEEN 6 preceding 
					AND 0 following
			) :: FLOAT ELSE NULL END AS deaths, 
			CASE WHEN Count(*) over(
				PARTITION BY location_id 
				ORDER BY 
					dt ROWS BETWEEN 6 preceding 
					AND 0 following
			) > 6 THEN Avg(recovered) over (
				PARTITION BY location_id 
				ORDER BY 
					dt ROWS BETWEEN 6 preceding 
					AND 0 following
			) :: FLOAT ELSE NULL END AS recovered 
		FROM 
			fact.fact_metrics
	) tmp 
WHERE 
	confirmed IS NOT NULL
