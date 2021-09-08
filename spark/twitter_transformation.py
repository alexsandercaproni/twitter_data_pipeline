from os.path import join
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def export_json(df, destination):
    df.coalesce(1).write.mode('overwrite').json(destination)


def get_users_data(df):
    return df.select(
        f.explode('includes.users').alias('users')
    ).select('users.*')


def get_tweets_data(df):
    return df.select(
            f.explode('data').alias('tweets')
        ).select(
            'tweets.author_id', 
            'tweets.conversation_id', 
            'tweets.created_at', 
            'tweets.id', 
            'tweets.in_reply_to_user_id', 
            'tweets.public_metrics.*', 
            'tweets.text')


def twitter_transform(spark, source, destination, process_date):
    df = spark.read.json(source)

    tweet_df = get_tweets_data(df)
    user_df = get_users_data(df)

    table_destination = join(destination, '{table_name}', f'process_date={process_date}')

    export_json(tweet_df, table_destination.format(table_name='tweet'))
    export_json(user_df, table_destination.format(table_name='user'))


if __name__ == '__main__':
    # Cria argumentos para no momento de execucao do spark
    parser = argparse.ArgumentParser(
        description='Spark Twitter Transformation'
    )
    parser.add_argument('--source', required=True)
    parser.add_argument('--destination', required=True)
    parser.add_argument('--process-date', required=True)
    args = parser.parse_args()

    # Sessao Spark
    spark = SparkSession\
        .builder\
        .appName('twitter_transformation')\
        .getOrCreate()
    
    # Transformacao
    twitter_transform(spark, args.source, args.destination, args.process_date)
