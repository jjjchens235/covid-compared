 
 <h3 align="center">Data Engineering: COVID19 Comparison Dashboard</h3>
<p align="center">
	A covid location comparison 
	<a href="https://dashboard.covid19compared.com/">dashboard</a>
	drawing from an automated data pipeline.
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

The main purpose of this data engineering project is to create an end to end automated data pipeline that will allow the end user to be able to easily query on three different covid fact metrics - confirmed, deaths, and recovered- across a number of different dimensions such as location and time. 

In order to showcase my work, I have also created a live covid dashboard that allows for regional comparisons across the aforementioned metrics, you can access it [here](https://dashboard.covid19compared.com).

Notable dashboard features:
- Allows comparisons across thes same location level, i.e Beijing, China vs Montana, United States
- Offers per capita and absolute numbers
- Top 5 most affected locations

### Built With
* [AWS RDS](https://aws.amazon.com/rds/)
* [Plotly Dash](https://plotly.com/dash/)
* [Apache Airflow](https://airflow.apache.org/)

## Data Pipeline Architecture 

(./images/flow_chart_mermaid_js2.png)

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
3. The end user can now access the updated data through the Dash app- deployed using Heroku and AWS Route 53.

#### Visualized as a DAG

(./images/dag.png)

## Prerequisites

### Data Pipeline Prerequisites
1. [Docker](https://docs.docker.com/get-docker/)
2. [docker-compose](https://docs.docker.com/compose/install/)
3. AWS account for S3 and RDS

### Dash App Prerequisites

#### Local
1. Connection to postgres database set-up from data pipeline
2. Python and all the Python libraries in requirements.txt

#### Server
1. All local prereqs
2. Heroku
3. Need to install the requirements.txt file in a virtual environment (more instructions below)

## Running Project

### Running data pipeline
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

### Running dash app

#### Running Locally
If first time running, copy the example file, and enter your credentials:
``` bash
cd covid_de/
cp dash_app/config/dash_credentials_example.json dash_app/config/dash_credentials.json
vi dash_app/config/dash_credentials.json
```

To deploy dash app locally run:
``` python
python app_dash.py
```

#### Deploying on Heroku
If first time deploying, create a new app in Heroku, and add the environment variables from the example file and their corresponding values into Heroku [Dashboard](https://dashboard.heroku.com/apps) -> Settings tab. More detailed instructions on adding env variables to Heroku [here](https://devcenter.heroku.com/articles/config-vars#using-the-heroku-dashboard).

To actually deploy, follow these [instructions](https://dash.plotly.com/deployment).
The only difference is that this Dash app is in a sub-folder, so when pushing, run this instead:
```bash
cd covid_de/
git subtree push --prefix dash_app/ heroku master
```

## AWS Lambda
This app was originally deployed on AWS Lambda using [zappa](https://github.com/Miserlou/Zappa).
Lambda's one flaw was significant inital website load times (up to 15s) due to [cold starts](https://towardsdatascience.com/avoiding-cold-starts-on-aws-lambda-for-a-long-running-api-request-15b8194f2e01).

Switching to Heroku ended up being really easy thankfully, but I left the 'zappa' branch up in case someone wants to refer to it as there aren't many examples of Dash + zappa deployments.

## Inspiration

I chose this specific topic because when I went on a trip in Fall 2020 from Southern California- where I live- to Dallas, I remember being curious about which location I was more likely to get covid in.  Getting the per capita number of cases for both locations manually was a pain, and as such this project was borne.

From a data engineering project perspective, I was inspired by this [Meetup data engineering project](https://josephwibowo.github.io/Meetup_Analytics/).


