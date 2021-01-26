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

https://github.com/othneildrew/Best-README-Template/blob/master/README.md
# Table of Contents
TODO

## About The Project
TODO: add a screenshot, add a link


This is a covid dashboard that allows for regional comparisons across several metrics and region levels (country, state, and county).
I want to become a data engineer so the main focus of this project was creating an automated data pipeline using airflow and hosting on the cloud via AWS. The dashboard itself was a way to showcase my work.


I chose this specific topic because when I went on a trip in fall 2020 from Southern California- where I live- to Dallas, I remember being curious which location I was more likely to get covid in.  Getting the per capita number of cases for both locations manually was a pain, so I decided to create this.

All the source data is from John Hopkin's [GitHub](https://github.com/CSSEGISandData/COVID-19).
I was inspired by this meetup [data engineering project](https://josephwibowo.github.io/Meetup_Analytics/).

### Built With
* [AWS rds](https://aws.amazon.com/rds/)
* [AWS lambda](https://aws.amazon.com/lambda/)
* [Plotly Dash](https://plotly.com/dash/)
* [Apache airflow](https://airflow.apache.org/)

## Data Pipeline Architecture 

(./images/flow_chart_mermaid_js.png)

####Pipeline steps
1. Python pandas script that cleans the data and stages it to AWS S3.
2. SQL scripts that:
	1. loads the data from S3 to RDS
	2. transform the data into a star schema
		1. 	fact table contains the 3 metrics (confirmed, deaths, recovered)
		2. the dimension tables are location, date, and coordinates.
	3. From the fact tables, create final bi tables (county, state, and country) that our end application will query from
3. Dash app deployed to the web using AWS lambda

####Visualized as a DAG:

(./images/dag.png)


## Getting Started
This project consists of two parts: 
1.  The data pipeline that moves the John Hopkin's [data](https://github.com/CSSEGISandData/COVID-19) to a RDS database.
2. The Dash App that displays the comparison dashboard.

### Prerequisites

#### Data Pipeline Prerequisites
1. [Docker](https://docs.docker.com/get-docker/)
2. [docker-compose](https://docs.docker.com/compose/install/)
3. AWS account for S3 and RDS

#### Dash App Prerequisites
1. If you want to host on the cloud, an AWS account for RDS and Lambda
2. Need to install the requirements.txt file in a virtual environment (more instructions below)

## Running Project

#### Running data pipeline
- Build project

``` sh
docker-compose up -d
```


- Setup airflow connection
After the 3 airflow servers are up, go to http://localhost:8080/ and create a new connection named 'rds'

TODO add a screenshot

- Delete project containers

``` sh
docker-compose down
```

### Running dash app
Two options
* Deploy dash app locally
``` python
python app_dash.py
```

* Deploy dash app on AWS lambda using python library zappa, follow these [instructions](https://pythonforundergradengineers.com/deploy-serverless-web-app-aws-lambda-zappa.html)



## Airflow
1. Adjust any pipeline/.env environment variables you see fit, this is what worked best for my machine. In particular, make sure you have created FERNET_KEY environment variable. Fernet key steps:
	1. `echo $FERNET_KEY` If this prints something, then nothing further needs to be done.
	2. Else, follow the steps here: https://beau.click/airflow/fernet-key
2. Inside pipeline/dags/covid_dag.py, consider changing the `start_date` to a more recent date, i.e. yesterday's date. Also feel free to change the `schedule_interval`.
3. Inside pipeline run `docker-compose up -d` to start the pipeline
4. Go to http://localhost:8080/, it will ask you for a username and password, type `admin` and `password`
5. You will get a NameError, saying `rds` is undefined. To fix this, inside the airflow UI, add a new connection `admin -> connection -> new connection (the plus sign)`. Fill in the following fields:
	1. conn id: rds
	2. host: databaseXX.XXXXXXX.us-XXXX.rds.amazonaws.com
	3. schema: default is postgres
	4. login: postgres
	5. password: <your password>
	6. port: 5432
6. Go back to main page and refresh until you see covid dag appear
7. Turn on dag and then run it

