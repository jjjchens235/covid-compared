

#entrypoint.sh
#!/usr/bin/env bash
airflow initdb
airflow webserver

#docker run -d -p 8080:8080 -v /Users/jwong/Programming/Python/DEngineering/covid_de/dags/:/usr/local/airflow/dags  puckel/docker-airflow webserver
