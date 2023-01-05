# Databricks notebook source
# DBTITLE 1,Load Utils
# MAGIC %run  /Shared/Utils/Utils_Dataprocessing

# COMMAND ----------

# DBTITLE 1,Write stream [Bronze]
decoded_df.writeStream.\
            format("delta").\
            outputMode("append").\
            option("checkpointLocation", "/delta/events/bronze/checkpoints/tweets").\
            start("/delta/bronze/tweets")

# COMMAND ----------

# DBTITLE 1,Write stream [Silver]
read_df_bronze.writeStream.\
            format("delta").\
            outputMode("append").\
            option("checkpointLocation", "/delta/events/silver/checkpoints/tweets").\
            start("/delta/silver/tweets")

# COMMAND ----------

# DBTITLE 1,Write stream [Gold]
s = sched.scheduler(time.time, time.sleep)
print("Processing streaming into gold layer")
def do_something(sc): 
    try:
        silver_df_20s=get_last_silver_data()
        if silver_df_20s.count()==0:
            pass
        else:        
            silver_df=get_nlp_predictions(silver_df_20s,schema)
            insert_into_gold(silver_df)      
        sc.enter(5, 1, do_something, (sc,))
    except Exception as e:        
        print(str(e))
s.enter(5, 1, do_something, (s,))
s.run()