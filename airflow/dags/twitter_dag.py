import sys
sys.path.insert(0, '/Users/alexsandercaproni/Documents/python_projects/twitter_data_pipeline')
from airflow.plugins.operator.twitter_operator import TwitterOperator

from datetime import datetime
from os.path import join

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import DAG


with DAG(dag_id='twitter_dag', start_date=datetime.now()) as dag:
    twitter_operator = TwitterOperator(
        task_id = 'twitter_neo',
        query='Neoenergia',
        file_path=join(
            '/Users/alexsandercaproni/Documents/python_projects/twitter_data_pipeline/datalake/bronze',
            'neo_tweets',
            'extract_date={{ ds }}',
            'Neoenergia_{{ ds_nodash }}.json'
        )
    )

    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_neo",
        application=(
            "/Users/alexsandercaproni/Documents/python_projects/twitter_data_pipeline/spark/twitter_transformation.py"
        ),
        name="twitter_transformation",
        application_args=[
            "--src",
            "/Users/alexsandercaproni/Documents/python_projects/twitter_data_pipeline/datalake/bronze/neo_tweets/extract_date=2021-08-29",
            "--dest",
            "/Users/alexsandercaproni/Documents/python_projects/twitter_data_pipeline/datalake/silver/neo_tweets",
            "--process-date",
            "{{ ds }}"
        ]
    )
