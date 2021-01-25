DROP TABLE IF EXISTS staging_us_confirmed;
DROP TABLE IF EXISTS staging_global_confirmed;

DROP TABLE IF EXISTS staging_us_deaths;
DROP TABLE IF EXISTS staging_global_deaths;

DROP TABLE IF EXISTS staging_global_recovered;
DROP TABLE IF EXISTS staging_location;

DROP TABLE IF EXISTS location;
DROP TABLE IF EXISTS iso2;
DROP TABLE IF EXISTS lat_lon;
DROP TABLE IF EXISTS time;


DROP TABLE IF EXISTS confirmed_temp;
DROP TABLE IF EXISTS deaths_temp;
DROP TABLE IF EXISTS recovered_temp;

DROP TABLE IF EXISTS fact_metrics;
DROP TABLE IF EXISTS fact_metrics_moving_avg;

DROP TABLE IF EXISTS bi_county;
DROP TABLE IF EXISTS bi_state;
DROP TABLE IF EXISTS bi_country;
