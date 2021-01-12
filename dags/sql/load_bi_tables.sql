-- insert into bi tables to be used for dash app

-- county
INSERT INTO bi_county (combined_key, country, state, county, population, dt, confirmed, deaths, recovered, confirmed_per_capita, deaths_per_capita, recovered_per_capita)
SELECT combined_key, country, state, county, population, dt, confirmed, deaths, recovered, 

round((100000.0 / cast(population as numeric) * cast(confirmed as numeric)), 2) confirmed_per_capita,
round((100000.0 / cast(population as numeric) * cast(deaths as numeric)), 2) deaths_per_capita,
round((100000.0 / cast(population as numeric) * cast(recovered as numeric)), 2) recovered_per_capita 

FROM fact_metrics_moving_avg f JOIN location l on f.location_id = l.location_id WHERE county IS NOT NULL; 

-- state
INSERT INTO bi_state (combined_key, country, state, population, dt, confirmed, deaths, recovered, confirmed_per_capita, deaths_per_capita, recovered_per_capita)
-- need to group by since US states are by county
SELECT combined_key, country, state, population, dt, confirmed, deaths, recovered, 

round((100000.0 / cast(population as numeric) * cast(confirmed as numeric)), 2) confirmed_per_capita,
round((100000.0 / cast(population as numeric) * cast(deaths as numeric)), 2) deaths_per_capita,
round((100000.0 / cast(population as numeric) * cast(recovered as numeric)), 2) recovered_per_capita 

FROM fact_metrics_moving_avg f JOIN location l on f.location_id = l.location_id WHERE state IS NOT NULL and county IS NULL;

-- country
INSERT INTO bi_country (combined_key, population, dt, confirmed, deaths, recovered, confirmed_per_capita, deaths_per_capita, recovered_per_capita)
-- need to group by since US states are by county
SELECT combined_key, population, dt, confirmed, deaths, recovered,

round((100000.0 / cast(population as numeric) * cast(confirmed as numeric)), 2) confirmed_per_capita,
round((100000.0 / cast(population as numeric) * cast(deaths as numeric)), 2) deaths_per_capita,
round((100000.0 / cast(population as numeric) * cast(recovered as numeric)), 2) recovered_per_capita 

FROM fact_metrics_moving_avg f JOIN location l on f.location_id = l.location_id WHERE county IS NULL and state IS NULL
