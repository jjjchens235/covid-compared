 
 <h3 align="center">Data Engineering: COVID19 Comparison Dashboard</h3>
<p align="center">
	A covid location comparison 
	<a href="https://dashboard.covid19compared.com/">dashboard</a>
	supported through an automated data pipeline.
</p>

(./images/dashboard_screenshot.png)

# Table of Contents
<details open="open">
	<summary>Table of Contents</summary>
	<ol>
		<li>
			<a href="#about-the-project">About The Project</a>
			<ul>
				<li><a href="#built-with">Built With</a></li>
			</ul>
		</li>
		<li>
			<a href="#data-pipeline-architecture">Data Pipeline Architecture</a>
		</li>
			<ul>
				<li><a href="#pipeline-steps">Pipeline steps</a></li>
				<li><a href="#visualized-as-a-dag">Visualized as a DAG</a></li>
			</ul>
		<li>
			<a href="#prerequisites">Prerequisites</a>
			<ul>
				<li><a href="#data-pipeline-prerequisites">Data Pipeline Prerequisites</a></li>
				<li><a href="#dash-app-prerequisites">Dash App Prerequisites</a></li>
			</ul>
		</li>
		<li><a href="#running-project">Running Project</a></li>
			<ul>
				<li><a href="#running-data-pipeline">Running data pipeline</a></li>
				<li><a href="#running-dash-app">Running dash app</a></li>
			</ul>
		<li><a href="#inspiration">Inspiration</a></li>
	</ol>
</details>


## About The Project

The main purpose of this project is to create an end to end automated data pipeline that will allow the end user to be able to easily query on three different covid fact metrics - confirmed, deaths, and recovered- across a number of different dimensions such as location and time. 

In order to showcase my work, I have also created a live covid dashboard that allows for regional comparisons across the aforementioned metrics, you can access it [here](https://dashboard.covid19compared.com).

### Built With
* [AWS RDS](https://aws.amazon.com/rds/)
* [AWS Lambda](https://aws.amazon.com/lambda/)
* [Plotly Dash](https://plotly.com/dash/)
* [Apache Airflow](https://airflow.apache.org/)

## Data Pipeline Architecture 

(./images/flow_chart_mermaid_js.png)

#### Pipeline steps
1. John Hopkins data -> S3 using Python pandas  
	1. The John Hopkins [repo](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series) organizes its time series files by metric (confirmed, deaths, recovered) and by location (global, and US).
	2. Each of these 5 files are cleaned using pandas and then staged to S3 using Python s3fs library.
2. SQL scripts that:
	1. extract the data from S3 to RDS.
	2. transform the data into a star schema.
		1. 	fact table contains the 3 metrics (confirmed, deaths, recovered).
		2. the dimension tables are location, date, and coordinates.
	3. From the fact tables, create final bi tables (county, state, and country) that our end application will query from.
3. The end user can now access the updated data through the Dash app- deployed using Lambda.

#### Visualized as a DAG

(./images/dag.png)

## Prerequisites

#### Data Pipeline Prerequisites
1. [Docker](https://docs.docker.com/get-docker/)
2. [docker-compose](https://docs.docker.com/compose/install/)
3. AWS account for S3 and RDS

#### Dash App Prerequisites

#### Local
1. Connection to postgres database set-up from data pipeline
2. Python and all the Python libraries in requirements.txt excluding zappa and its dependencies

#### Server
1. An AWS account for Lambda
2. Need to install the requirements.txt file in a virtual environment (more instructions below)

## Running Project

#### Running data pipeline
- If first time running, add credentials to `pipeline/dags/config/aws_config.json`. An example file is provided for reference.

- Build project

``` sh
docker-compose up -d
```


* After the  airflow servers are up
	- go to http://localhost:8080/
		- username: admin
		- pw: password 
	- create a new connection named 'rds' like so:

(./images/airflow_admin_connections.png)
(./images/airflow_rds.png)

- To turn off airflow servers and delete Docker containers

``` sh
docker-compose down
```

#### Running dash app
- If first time running, add credentials to `dash_app/config/dash_app.cfg`, an example file is provided for reference.

* Deploy dash app locally
``` python
python app_dash.py
```

* Alternatively, to deploy Dash app on AWS Lambda using [zappa](https://github.com/Miserlou/Zappa), follow these [instructions](https://pythonforundergradengineers.com/deploy-serverless-web-app-aws-lambda-zappa.html)

## Inspiration

I chose this specific topic because when I went on a trip in Fall 2020 from Southern California- where I live- to Dallas, I remember being curious about which location I was more likely to get covid in.  Getting the per capita number of cases for both locations manually was a pain, and as such this project was borne.

From a data engineering project perspective, I was inspired by this [Meetup data engineering project](https://josephwibowo.github.io/Meetup_Analytics/).


