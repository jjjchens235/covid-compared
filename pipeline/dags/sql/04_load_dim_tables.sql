-- Insert dim location table
INSERT INTO dim.location
						(location_id,
						 country,
						 state,
						 county,
						 population,
						 combined_key)
SELECT location_id,
			 country,
			 state,
			 county,
			 Cast(population AS INT) population,
			 combined_key
FROM   staging.staging_location;

--insert into iso2 table
INSERT INTO dim.iso2
						(iso2,
						 location_id)
SELECT iso2,
			 location_id
FROM   staging.staging_location
WHERE  iso2 IS NOT NULL;

--insert into lat_lon table
INSERT INTO dim.lat_lon
						(lat,
						 lon,
						 location_id)
SELECT lat,
			 lon,
			 location_id
FROM   staging.staging_location;

-- Insert dim time table
INSERT INTO dim.time
						(dt,
						 year,
						 month,
						 day,
						 weekday)
SELECT DISTINCT dt,
								Extract(year FROM dt)  AS year,
								Extract(month FROM dt) AS month,
								Extract(day FROM dt)   AS day,
								Extract(dow FROM dt)   AS weekday
FROM   staging.staging_global_confirmed; 
