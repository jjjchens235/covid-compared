DROP TABLE IF EXISTS staging.staging_us_confirmed;
DROP TABLE IF EXISTS staging.staging_global_confirmed;

DROP TABLE IF EXISTS staging.staging_us_deaths;
DROP TABLE IF EXISTS staging.staging_global_deaths;

DROP TABLE IF EXISTS staging.staging_global_recovered;
DROP TABLE IF EXISTS staging.staging_location;

DROP TABLE IF EXISTS dim.location;
DROP TABLE IF EXISTS dim.iso2;
DROP TABLE IF EXISTS dim.lat_lon;
DROP TABLE IF EXISTS dim.time;


DROP TABLE IF EXISTS temp.confirmed_temp;
DROP TABLE IF EXISTS temp.deaths_temp;
DROP TABLE IF EXISTS temp.recovered_temp;

DROP TABLE IF EXISTS fact.fact_metrics;
DROP TABLE IF EXISTS fact.fact_metrics_moving_avg;

