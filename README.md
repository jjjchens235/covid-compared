# Purpose
This data pipeline extracts covid data from S3, loads it into AWS Redshift where the data is then transformed into a star schema. The goal of this pipeline is to allow the end user to be able to easily query on three different fact metrics - confirmed, deaths, and recovered across a number of different dimensions such as county, state, country, and time so that they can perform further analysis. 

I have tried to generalize the fact tables to be used by anyone, but for me personally, I plan on creating some dashboards that will allow a user to compare different locations (county, state, country) against each other.

## To start data pipeline
1. Create a redshift cluster in AWS
2. Update dwh.cfg with the proper credentials
3. Run create_tables.py
4. Run etl.py

## Steps
1. Scope the Project and Gather Data
	1. The source files are from this link: https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series
	2. This raw github data is transformed in a pandas script and loaded into S3. The files in S3 are the source files that this project uses.
2. Explore and Assess the Data
	1. My goal was to find daily data for confirmed and death metrics by date, which this data source has.
3. Define the Data Model
	1. The end I had in mind was a dashboard where the user could compare different counties, states, or countries against each other for each metric over time.
	2. With this end a mind, the data model I will be using is star schema with two fact tables surrounded by four dim tables
	3. The fact tables will have the three main metrics: confirmed, deaths, and recovered.
	4. The dimension tables will be county, state, country, and time
	5. The reason why I chose a star schema
		1. Makes querying simpler. Will be able to easily adjust the joins based on what the user selects in the dashboard for either the metric(s) or locations to compare.
		2. Queries will be faster due to the denormalized nature of the schema
4. Run ETL to Model the Data
	1. The raw data comes in the form of 5 different .csv files located in S3. The files are split by metric (confirmed, deaths, recovered) and by places (global, and US). The first set of queries create staging tables in AWS Redshift for these 5 files.
	2. The second set of queries creates the dimension tables (county, state, country, and time)
	3. The third set of queries takes the 5 staging tables and transforms them to 5 temp tables by adding foreign keys from the dim tables.
	4. The fourth set of queries takes the 5 temp tables and coalesces the facts from each of the temp tables into one table. The end result are two fact tables, one by county (US only), and one global table by country and state.
5. Complete Project Write Up

## Other Scenarios
1. What happens if the data was scaled by 100x? Currently I'm using 1 DC2.Large node, I would simply scale to 2 DC2.XLarge nodes and see if performance is acceptable, and scale up more if necessary.
2. What happens if the data pipelines needs to be run on a daily basis at 7am? I would use Airflow to schedule this.
3. What happens if the database needed to be accessed by 100+ people? The more people accessing the database the more CPU resources you need to get a fast experience. By using a distributed database you can improve your replications and partitioning to get faster query results for each user.

