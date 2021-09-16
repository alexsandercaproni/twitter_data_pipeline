from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum, to_date, date_format, countDistinct


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("twitter_insight_tweet")\
        .getOrCreate()
    
    tweet = spark.read.json(
        "/Users/alexsandercaproni/Documents/python_projects/"
        "twitter_data_pipeline/datalake/silver/neo_tweets/tweet"
    )

    neo = tweet\
        .where("author_id in ('1057341729714651141', '2731564592', '2873781071', '2873766555', '2314129536', '2731582484')")\
        .select("author_id", "conversation_id")

    tweet = tweet.alias("tweet")\
        .join(
            neo.alias("neo"),
            [
                neo.author_id != tweet.author_id,
                neo.conversation_id == tweet.conversation_id
            ],
            'left')\
        .withColumn(
            "neo_conversation",
            when(col("neo.conversation_id").isNotNull(), 1).otherwise(0)
        ).withColumn(
            "reply_neo",
            when((col("tweet.in_reply_to_user_id") == '1057341729714651141') |
             (col("tweet.in_reply_to_user_id") == '2731564592') | 
             (col("tweet.in_reply_to_user_id") == '2873781071') | 
             (col("tweet.in_reply_to_user_id") == '2873766555') | 
             (col("tweet.in_reply_to_user_id") == '2314129536') | 
             (col("tweet.in_reply_to_user_id") == '2731582484'), 1).otherwise(0)
        ).groupBy(to_date("created_at").alias("created_date"))\
        .agg(
            countDistinct("id").alias("n_tweets"),
            countDistinct("tweet.conversation_id").alias("n_conversation"),
            sum("neo_conversation").alias("neo_conversation"),
            sum("reply_neo").alias("reply_neo")
        ).withColumn("weekday", date_format("created_date", "E"))\
        .orderBy("created_date")
    
    
    tweet.coalesce(1)\
        .write.mode("overwrite")\
        .json("/Users/alexsandercaproni/Documents/python_projects/"
              "twitter_data_pipeline/datalake/gold/twitter_insight_tweet")
