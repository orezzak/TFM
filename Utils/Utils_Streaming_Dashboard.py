# Databricks notebook source
# DBTITLE 1,Libraries
import requests
import json
import sched, time

# COMMAND ----------

# DBTITLE 1,Generic functions
def get_sentiment_volume (author_id,sentiment_type):
    query=f'''select count(*) from gold.tweets where author_id= '{author_id}' and sentiment_analysis = '{sentiment_type}' '''
    result=str(spark.sql(query).collect()[0][0])
    return result

def get_emotions_topic_volume(author_id,col):
    query=f'''select count(*) value, {col} from gold.tweets where author_id= {author_id} and sentiment_analysis is not null  group by 2'''
    emotion_analysis_volume=spark.sql(query).toJSON().map(json.loads).collect()
    return emotion_analysis_volume

# def get_gold_table_content():
#     query=f'''select * from gold.tweets
#                                     where author_id is not null 
#                                     and sentiment_analysis is not null   
#                                     and cast(created_at as TIMESTAMP)> cast(current_timestamp as TIMESTAMP) + INTERVAL -60 seconds'''
#     gold_table_content=spark.sql(query).toJSON().map(json.loads).collect()
#     return gold_table_content

def send_to_powerbi_streaming_ds(payload,url):   
    response = requests.request("POST", url,data=json.dumps(payload))
    return print(response.text)
  
def get_tweets_volume():
    query=f'''select  count(*) value, concat(substring(created_at,1,17),'00.000Z') created_at from silver.tweets                                 
                                  where cast(created_at as TIMESTAMP)> cast(current_timestamp as TIMESTAMP) + INTERVAL -30 seconds
                                  group by concat(substring(created_at,1,17),'00.000Z') order by 2 desc limit 1'''
    tweets_volume=spark.sql(query).toJSON().map(json.loads).collect()
    return tweets_volume
    

# COMMAND ----------

# DBTITLE 1,Streaming dataset Urls
url_sentiment="https://api.powerbi.com/beta/aec762e4-3d54-495e-a8fe-4287dce6fe69/datasets/121aac46-9729-438e-9968-3b1775481650/rows?key=GUhlbc8GzgVjeDj5kO4qd%2FbwW8FUui%2FkdED8ipEVvieXkD9GfUrwvEZwyVV54JqlqiaqJ4dC5XWXpPvfmJjDPXXD"

url_emotion="https://api.powerbi.com/beta/aec762e4-3d54-495e-a8fe-4287dce6fe69/datasets/94def608-96e0-4dcd-920d-0f8ff402feb9/rows?key=EB2u39f01aFQMnnTSgM95bE8GTzG3Cjx1aVmWqOI2F3Zjb1hmArO9Lj1ESnNR59Co%2BZYwiFzAZOhJVRog4q6hw%3XX"

url_topic="https://api.powerbi.com/beta/aec762e4-3d54-495e-a8fe-4287dce6fe69/datasets/3fcc445c-24c0-4bc9-8feb-00528c6842f5/rows?key=GF3QqHNISwCwbGc93bd0G1%2BEPRNSZCLgadhBSYrC7%2FIDcGsJiVdBmGQ7SlmdGiTqp3zJDnnSmJutpBHYxtph8Q%XX"

url_hybrid="https://api.powerbi.com/beta/aec762e4-3d54-495e-a8fe-4287dce6fe69/datasets/22eb0fca-534a-4888-909b-6a42b85323e6/rows?key=iyu1f9387rKqLSTCoH16FT6fHG8NMO7eNBl68%2BLos8YrQel7dr5J8I%2FmoEEOruidd4CqD2AP9Cjjtv%2Bri2wWVXX"

url_volume="https://api.powerbi.com/beta/aec762e4-3d54-495e-a8fe-4287dce6fe69/datasets/0f0c2710-b527-4abb-afe5-09e8911849ac/rows?key=gZUs8kTMsCqiz15A60qoblesEUldoDrmXw23oiVXGDwvTfvFXxclvBYjM6xZsXy4qJp5gEKJMqHiqRhRShCUJXX"