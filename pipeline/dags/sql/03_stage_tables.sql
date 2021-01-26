
-- aws_s3_extension 
CREATE EXTENSION IF NOT EXISTS aws_s3 cascade;

-- staging_us_confirmed_copy
SELECT aws_s3.table_import_from_s3(
	 'staging.staging_us_confirmed',
	 'country, state, county, dt, confirmed',
	 '(FORMAT csv, DELIMITER E''\t'', HEADER true)',
	 aws_commons.create_s3_uri( '{{ params.s3_bucket }}', '{{ params.us_confirmed }}', '{{ params.region }}' )
);

-- staging_global_confirmed_copy
SELECT aws_s3.table_import_from_s3(
	 'staging.staging_global_confirmed',
	 'country, state, dt, confirmed',
	 '(FORMAT csv, DELIMITER E''\t'', HEADER true)',
	 aws_commons.create_s3_uri( '{{ params.s3_bucket }}', '{{ params.global_confirmed }}', '{{ params.region }}' )
);

--staging_us_deaths_copy
SELECT aws_s3.table_import_from_s3(
	 'staging.staging_us_deaths',
	 'country, state, county, dt, deaths',
	 '(FORMAT csv, DELIMITER E''\t'', HEADER true)',
	 aws_commons.create_s3_uri( '{{ params.s3_bucket }}', '{{ params.us_deaths }}', '{{ params.region }}' )
);




--staging_global_deaths_copy
SELECT aws_s3.table_import_from_s3(
	 'staging.staging_global_deaths',
	 'country, state, dt, deaths',
	 '(FORMAT csv, DELIMITER E''\t'', HEADER true)',
	 aws_commons.create_s3_uri( '{{ params.s3_bucket }}', '{{ params.global_deaths }}', '{{ params.region }}' )
);



-- staging_global_recovered_copy
SELECT aws_s3.table_import_from_s3(
	 'staging.staging_global_recovered',
	 'country, state, dt, recovered',
	 '(FORMAT csv, DELIMITER E''\t'', HEADER true)',
	 aws_commons.create_s3_uri( '{{ params.s3_bucket }}', '{{ params.global_recovered }}', '{{ params.region }}' )
);


SELECT aws_s3.table_import_from_s3(
	 'staging.staging_location',
	 'location_id, country, state, iso2, county, population, lat, lon, combined_key',
	 '(FORMAT csv, DELIMITER E''\t'', HEADER true)',
	 aws_commons.create_s3_uri( '{{ params.s3_bucket }}', '{{ params.location }}', '{{ params.region }}' )
);
