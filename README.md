# Twitter Data Pipeline
This is a twitter data pipeline using Airflow


## Configurations and Installations:

### Creating and access your venv:
1. python3 -m venv sandbox
2. source sandbox/bin/activate
3. pip install wheel
4. pip3 install apache-airflow==2.1.0 --constraint https://gist.githubusercontent.com/marclamberti/742efaef5b2d94f44666b0aec020be7c/raw/21c88601337250b6fd93f1adceb55282fb07b7ed/constraint.txt

### Initializing Airflow:
In your sandbox, run the following commands:

1. airflow db init, to start the database
2. airflow webserver, to start the UI and airflow server
3. airflow scheduler, to start airflow scheduler

### Creating admin user
In airflow 2.0+, it's necessary create an user to access Airflow UI.

1. airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin


### Install providers
We use two providers in this example. To install the, run the follow commands in terminal:
1. pip3 install apache-airflow-providers-http
2. pip3 install apache-airflow-providers-apache-spark


## Steps to Reproduce:

### Connections
We need to create two connections in Airflow UI:

#### twitter_default:
**Conn Id:** twitter_default
**Conn Type:** HTTP
**Host:** https://api.twitter.com
**Extras:** {"Authorization": "Bearer **MY_BEARER_CODE**"}


#### spark_default:
**Conn Id:** spark_default
**Conn Type:** Spark
**Host:** local
**Extras:** {"spark-home": **"PATH_SPARK"**}

