import sys
sys.path.insert(0, '/Users/alexsandercaproni/Documents/Python Projects/twitter_pipeline')
from plugins.operators.twitter_operator import TwitterOperator

from airflow.models import DAG
from datetime import datetime
from os.path import join


with DAG(dag_id='twitter_dag', start_date=datetime.now()) as dag:
    twitter_operator = TwitterOperator(
        task_id = 'twitter_neo',
        query='Neoenergia',
        file_path=join(
            '/Users/alexsandercaproni/Documents/Python Projects/twitter_pipeline/datalake',
            'neo_tweets',
            'extract_date={{ ds }}',
            'Neoenergia_{{ ds_nodash }}.json'
        )
    )
