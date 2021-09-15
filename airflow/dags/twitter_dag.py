import sys
sys.path.insert(0, '/Users/alexsandercaproni/Documents/python_projects/twitter_data_pipeline')
from airflow.plugins.operator.twitter_operator import TwitterOperator

from datetime import datetime
from os.path import join
from pathlib import Path

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago


ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(6),
}

BASE_FOLDER = join(
    str(Path("~/Documents").expanduser()),
    "python_projects/twitter_data_pipeline/datalake/{stage}/neo_tweets/{partition}"
)

SPARK_SCRIPT_FOLDER = join(
    str(Path("~/Documents").expanduser()),
    "python_projects/twitter_data_pipeline/spark"
)

PARTITION_FOLDER = "extract_date={{ ds }}"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"


with DAG(
    dag_id = "twitter_dag", 
    default_args=ARGS,
    schedule_interval="0 9 * * *",
    max_active_runs=1
) as dag:
    twitter_operator = TwitterOperator(
        task_id = "twitter_neo",
        query = "Neoenergia",
        file_path=join(
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "Neoenergia_{{ ds_nodash }}.json"
        ),
        start_time=(
            "{{"
            f" execution_date.strftime('{ TIMESTAMP_FORMAT }') "
            "}}"
        ),
        end_time=(
            "{{"
            f" next_execution_date.strftime('{ TIMESTAMP_FORMAT }') "
            "}}"
        )
    )

    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_neo",
        application=join(
            SPARK_SCRIPT_FOLDER,
            "twitter_transformation.py"
        ),
        name="twitter_transformation",
        application_args=[
            "--src",
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "--dest",
            BASE_FOLDER.format(stage="silver", partition=""),
            "--process-date",
            "{{ ds }}",
        ]
    )

    # Criando dependencia entre as tasks
    twitter_operator >> twitter_transform
