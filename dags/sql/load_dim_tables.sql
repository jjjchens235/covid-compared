

-- Insert dim location table
INSERT INTO location (country, state, iso2, county, population, state_country, county_state_country)
SELECT DISTINCT country, state, iso2, county, CAST(population as INT) population, concat(state, country) state_country, concat(county, state, country) county_state_country
FROM staging_us_confirmed

UNION

SELECT DISTINCT country, state, NULL as iso2, NULL as county, CAST(population as INT), concat(state, country) state_country, concat(state, country) county_state_country
FROM staging_global_confirmed;


-- Insert dim time table
INSERT INTO time (dt, year, month, day, weekday)
SELECT distinct dt, extract(year FROM dt) as year, extract(month FROM dt) as month, extract(day FROM dt) as day, extract(dow FROM dt) as weekday 
FROM staging_global_confirmed;
