from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName('twitter_transformation')\
        .getOrCreate()
    
    df = spark.read.json(
        '/Users/alexsandercaproni/Documents/Python Projects/twitter_pipeline/datalake/neo_tweets'
    )

    df.printSchema()
    df.show()
