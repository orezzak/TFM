# Databricks notebook source
# DBTITLE 1,Libraries
#Tweeter Streaming
import tweepy
import json
from pyspark.sql.functions import  *

# COMMAND ----------

# DBTITLE 1,Credentials
# assign the values accordingly
consumer_key = "XXXX"
consumer_secret = "XXXX"
access_token = "XXXX"
access_token_secret = "XXX"
bearer_token ='XXXXX'
client=tweepy.Client(bearer_token=bearer_token,return_type=dict)

# COMMAND ----------

# DBTITLE 1,Generic functions
def get_user_mentions(user_id):
    Mentions=client.get_users_mentions(id=user_id,expansions='author_id',max_results=10)
    mentions_df=spark.createDataFrame(Mentions['data'])
    mentions_df=mentions_df.select('author_id').withColumn('target_id',lit(user_id))    
    return mentions_df

def get_relationships(user_id_list):    
    step=0
    for i in user_id_list:
        try:
            step=step+1
            user_id=i[0]
            if step==1:
                df=get_user_mentions(user_id)
            else:
                df2=get_user_mentions(user_id)
                df=df.union(df2)
        except Exception as e:
            pass
    return df


def get_user_id_by_name(user_name_list):
    users=client.get_users(usernames=user_name_list)['data']
    user_details_df=spark.createDataFrame(users)
    return user_details_df

def get_user_name_by_id(user_id_list):
    users=client.get_users(ids=user_id_list[:100])['data']
    user_details_df=spark.createDataFrame(users)
    return user_details_df
    

# COMMAND ----------

# DBTITLE 1,Get user id by user name [Master]
user_names=['fifaworldcup_es','pelotu2attender']
master_user_id_list=get_user_id_by_name(user_names).select('id').collect()

# COMMAND ----------

# DBTITLE 1,Get users relationships Master and slaves [Fact relationships Table]
#Get users relationships Master and slaves
df_master_relationships=get_relationships(master_user_id_list)
slave_id_list=df_master_relationships.select('author_id').collect()
df_slave_relationships=get_relationships(slave_id_list)
df_relationship=df_master_relationships.union(df_slave_relationships)

#Insert into delta table
df_relationship.write.format("delta").mode("overwrite").save('/delta/silver/user_mentions')


# COMMAND ----------

# DBTITLE 1,Get user names by user id [Dim users Table]


source_user_id=df_relationship.select('author_id').distinct()
source_user_ids=source_user_id.rdd.map(lambda x: x.author_id).collect()
source_user_ids=get_user_name_by_id(source_user_ids)

target_user_id=df_relationship.select('target_id').distinct()
target_user_ids=target_user_id.rdd.map(lambda x: x.target_id).collect()
target_user_ids=get_user_name_by_id(target_user_ids)


df_users=target_user_ids.union(source_user_ids).distinct()
#Insert into delta table
df_users.write.format("delta").mode("overwrite").save('/delta/silver/dim_users')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.user_mentions

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.dim_users