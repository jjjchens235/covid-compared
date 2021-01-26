
-- Insert dim location table
INSERT INTO dim.location (location_id, country, state, county, population, combined_key)
SELECT location_id, country, state, county, CAST(population as INT) population, combined_key FROM staging.staging_location;

--insert into iso2 table
INSERT INTO dim.iso2 (iso2, location_id)
SELECT iso2, location_id FROM staging.staging_location WHERE iso2 is NOT NULL;

--insert into lat_lon table
INSERT INTO dim.lat_lon (lat, lon, location_id)
SELECT lat, lon, location_id FROM staging.staging_location;

-- Insert dim time table
INSERT INTO dim.time (dt, year, month, day, weekday)
SELECT distinct dt, extract(year FROM dt) as year, extract(month FROM dt) as month, extract(day FROM dt) as day, extract(dow FROM dt) as weekday 
FROM staging.staging_global_confirmed;
