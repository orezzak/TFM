# Databricks notebook source
# DBTITLE 1,Load Utils
# MAGIC %run  /Shared/Utils/Utils_Streaming_Dashboard

# COMMAND ----------

# DBTITLE 1,Set parameters
dbutils.widgets.text("author_id", "", "author_id")
author_id = dbutils.widgets.get("author_id")

# COMMAND ----------

# DBTITLE 1,Send to streaming Dashboard
s = sched.scheduler(time.time, time.sleep)
print("Send to streaming dashboard")
def do_something(sc):     
    Sentiment_payload= [{
                        "author_id" :author_id,
                        "Positive" :get_sentiment_volume(author_id,'POS'),
                        "Negative" :get_sentiment_volume(author_id,'NEG'),
                        "Neutral" :get_sentiment_volume(author_id,'NEU')
                        }]
    send_to_powerbi_streaming_ds(Sentiment_payload,url_sentiment)

    #Emotion 
    Emotion_payload=get_emotions_topic_volume(author_id,'emotion_analysis')
    send_to_powerbi_streaming_ds(Emotion_payload,url_emotion)

    #Topic
    Topic_payload=get_emotions_topic_volume(author_id,'topic_detector')
    send_to_powerbi_streaming_ds(Topic_payload,url_topic)
    sc.enter(5, 1, do_something, (sc,))

s.enter(5, 1, do_something, (s,))
s.run()