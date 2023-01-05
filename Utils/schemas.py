# Databricks notebook source
# DBTITLE 1,Schemas
read_schema = StructType([
 StructField('data', StructType
             ([
                 StructField("author_id", StringType(), True),
                 StructField("created_at", StringType(), True),
                 StructField("edit_history_tweet_ids", StringType(), True),
                 StructField("geo", StringType(), True),
                 StructField("lang", StringType(), True),
                 StructField("source", StringType(), True),
                 StructField("text", StringType(), True),
                 StructField("matching_rules", StringType(), True),
                 StructField("tag", StringType(), True)
                 
                
             ]) )
             
           
])

schema = StructType([
    StructField("author_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("edit_history_tweet_ids", StringType(), True),
    StructField("geo", StringType(), True),
    StructField("source", StringType(), True),
    StructField("lang", StringType(), True),
    StructField("text", StringType(), True),
    StructField("matching_rules", StringType(), True),
    StructField("tag", StringType(), True),
    StructField("text_en", StringType(), True),
    StructField("sentiment_analysis", StringType(), True),
    StructField("emotion_analysis", StringType(), True),
    StructField("topic_detector", StringType(), True)
])