# Databricks notebook source
# DBTITLE 1,Load Utils
# MAGIC %run  /Shared/Utils/Utils_Streaming_Dashboard

# COMMAND ----------

# DBTITLE 1,Send Near Real Data
s = sched.scheduler(time.time, time.sleep)
print("Send near real data 30s")
def do_something(sc):   
    
    #Tweets volume
    tweets_volume=get_tweets_volume()
    if len(tweets_volume)==0:
        pass
    else:        
        send_to_powerbi_streaming_ds(tweets_volume,url_volume)
   
    sc.enter(20, 1, do_something, (sc,))

s.enter(20, 1, do_something, (s,))
s.run()