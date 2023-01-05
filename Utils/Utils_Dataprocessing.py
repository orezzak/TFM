# Databricks notebook source
print("Running Utils...")

# COMMAND ----------

# DBTITLE 1,Libraries
import json
from pyspark.sql.types import *
import  pyspark.sql.functions as F
from googletrans import Translator
import emoji
from transformers import pipeline
import sched, time
import pandas as pd
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Read Shemas
# MAGIC %run  /Shared/Utils/schemas

# COMMAND ----------

# DBTITLE 1,Drop tables
# MAGIC %sql
# MAGIC Drop table if exists bronze.tweets;
# MAGIC Drop table if exists silver.tweets;
# MAGIC Drop table if exists gold.tweets

# COMMAND ----------

# DBTITLE 1,Drop locations
dbutils.fs.rm("delta/events/bronze",True)
dbutils.fs.rm("delta/bronze/tweets",True)

dbutils.fs.rm("delta/events/silver",True)
dbutils.fs.rm("delta/silver/tweets",True)

dbutils.fs.rm("delta/events/gold",True)
dbutils.fs.rm("delta/gold/tweets",True)

print("Creating delta tables...")

# COMMAND ----------

# DBTITLE 1,Create delta tables
# MAGIC %sql
# MAGIC CREATE TABLE `bronze`.`tweets` 
# MAGIC (   
# MAGIC `author_id` STRING,   
# MAGIC `created_at` STRING,  
# MAGIC `edit_history_tweet_ids` STRING,  
# MAGIC `geo` STRING,   `lang` STRING,  
# MAGIC `source` STRING,   `text` STRING,  
# MAGIC `matching_rules` STRING,   
# MAGIC `tag` STRING
# MAGIC ) 
# MAGIC USING delta LOCATION 'dbfs:/delta/bronze/tweets';
# MAGIC 
# MAGIC CREATE TABLE 
# MAGIC `silver`.`tweets` 
# MAGIC (   
# MAGIC `author_id` STRING,  
# MAGIC `created_at` STRING,  
# MAGIC `edit_history_tweet_ids` STRING, 
# MAGIC `geo` STRING,   `lang` STRING,  
# MAGIC `source` STRING, 
# MAGIC `text` STRING,
# MAGIC `matching_rules` STRING,  
# MAGIC `tag` STRING, 
# MAGIC `text_en` STRING
# MAGIC )
# MAGIC USING delta LOCATION 'dbfs:/delta/silver/tweets';
# MAGIC 
# MAGIC CREATE TABLE 
# MAGIC `gold`.`tweets`
# MAGIC (   
# MAGIC `author_id` STRING,  
# MAGIC `created_at` STRING,  
# MAGIC `edit_history_tweet_ids` STRING,  
# MAGIC `geo` STRING,   `lang` STRING, 
# MAGIC `source` STRING,   `text` STRING,  
# MAGIC `matching_rules` STRING, 
# MAGIC `tag` STRING,  
# MAGIC `text_en` STRING,  
# MAGIC `sentiment_analysis` STRING, 
# MAGIC `emotion_analysis` STRING, 
# MAGIC `topic_detector` STRING
# MAGIC ) 
# MAGIC USING delta LOCATION 'dbfs:/delta/gold/tweets'

# COMMAND ----------

# DBTITLE 1,Event hub conf
print("Loading event hub configurarion...")

# Event Hub conf
connectionString = "Endpoint=sb://XXXX.servicebus.windows.net/;SharedAccessKeyName=TwitterAdmin;SharedAccessKey=XXXXXX=;EntityPath=XXX"
ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
ehConf['eventhubs.consumerGroup'] = "$Default"

# COMMAND ----------

# DBTITLE 1,Generic functions
print ("Loading generic functions")

def language_norm(texto):
    translator = Translator()
    xx=translator.translate(texto, dest='en')
    return str(xx.text)

language_normUDF = F.udf(lambda z:language_norm(z),StringType())

def get_emotion_analysis(texto):
    result=emotion_analysis(texto)
    return str(result[0]['label'])

get_emotion_analysisUDF = F.udf(lambda z:get_emotion_analysis(z),StringType())

###sentiment_analysis
def get_sentiment_analysis(texto):
    result=sentiment_analysis(texto)
    return str(result[0]['label'])

get_sentiment_analysisUDF = F.udf(lambda z:get_sentiment_analysis(z),StringType())

##Text classification
def get_text_classifier(texto):
    result=text_classifier(texto)
    return str(result[0]['label'])

get_text_classifierUDF = F.udf(lambda z:get_text_classifier(z),StringType())

def get_texten_fromdf(df):
    text_result_list=[]
    text_en_list=df.select("text_en").collect()
    for i in text_en_list:
        text_result_list.append(i[0])
    return text_result_list


def get_nlp_predictions(df,schema):
    
    text_result_list=[]
    text_en_list=df.select("text_en").collect()
    for i in text_en_list:
        text_result_list.append(i[0])
    
    
    df_1=spark.createDataFrame(sentiment_analysis(text_result_list)).select("label").withColumnRenamed("label","sentiment_analysis")
    df_2=spark.createDataFrame(emotion_analysis(text_result_list)).select("label").withColumnRenamed("label","emotion_analysis")
    df_3=spark.createDataFrame(text_classifier(text_result_list)).select("label").withColumnRenamed("label","topic_detector")
    
    df_prediction= pd.concat([df_1.toPandas(), df_2.toPandas(),df_3.toPandas()], axis=1)
    df_final=pd.concat([df.toPandas(),df_prediction],axis=1)
    
    return spark.createDataFrame(df_final,schema=schema).drop('id')

gold_tweets = DeltaTable.forPath(spark, '/delta/gold/tweets')

def get_last_silver_data():
    query=f'''
            select * from silver.tweets  
            where cast(created_at as TIMESTAMP)> cast(current_timestamp as TIMESTAMP) + INTERVAL -60 Seconds'''
    result=spark.sql(query)
    return result

def insert_into_gold(df):
    gold_tweets.alias("gold").merge(
    df.alias("df"),
    "gold.author_id = df.author_id and gold.created_at = df.created_at") \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

# DBTITLE 1,Read stream
print("Creating structured streamigd dataframes...")

## Bronze
#Read stream
read_df = (
      spark
        .readStream
        .format("eventhubs")
        .options(**ehConf)
        .load())
decoded_df = read_df.select(F.from_json(F.col("body").cast("string"), read_schema).alias("payload"))
decoded_df=decoded_df.select('*', "payload.data.*").drop('payload')


## Silver
read_df_bronze = (
          spark
            .readStream
            .format("delta")
            .option("ignoreDeletes", "true")
            .load('/delta/bronze/tweets'))

read_df_bronze=read_df_bronze.withColumn('text_en', language_normUDF(F.col('text')))

# COMMAND ----------

# DBTITLE 1,Huggin Face models
print("Downloading Huggin Face Models ...")
      
sentiment_analysis = pipeline("sentiment-analysis",model="finiteautomata/bertweet-base-sentiment-analysis")
emotion_analysis=pipeline("text-classification", model = "joeddav/distilbert-base-uncased-go-emotions-student")
text_classifier = pipeline("text-classification", model = "cardiffnlp/tweet-topic-21-multi")
# mental_health_analysis=pipeline("text-classification", model = "rabiaqayyum/autotrain-mental-health-analysis-752423172")
# hate_speech=pipeline("text-classification", model = "pysentimiento/bertweet-hate-speech")

# COMMAND ----------

print("Done")