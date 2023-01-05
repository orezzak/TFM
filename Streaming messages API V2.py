# Databricks notebook source
#Tweeter Streaming
import tweepy
import json
from datetime import datetime

####Event HUb
import asyncio
import nest_asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

## ADLS
from azure.storage.filedatalake import DataLakeServiceClient

# COMMAND ----------

# Send event to event Hub
async def run(message):
    # create a producer client to send messages to the event hub
    # specify connection string to your event hubs namespace and
        # the event hub name
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://XXXX.servicebus.windows.net/;SharedAccessKeyName=TwitterAdmin;SharedAccessKey=XXXXX=;EntityPath=twittermessages", eventhub_name="XXXXX")
    async with producer:
        # create a batch
        event_data_batch = await producer.create_batch()

        # add events to the batch
        event_data_batch.add(EventData(message))
        # send the batch of events to the event hub
        await producer.send_batch(event_data_batch)
 
def send_event(message):        
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(message))

# COMMAND ----------

#Upload file to Datalake Gen2
def get_adls_details():   

    # Get the below details from your storage account
    storage_account_name = "datalakegen2tfm"
    storage_account_key = "XXXXX"
    

    # Set ADLS Client details

    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    return service_client


def upload_adls_file(content): 
    
    service_client=get_adls_details()

    container_name = "raw"
    directory_name = " Streaming"

    file_system_client = service_client.get_file_system_client(file_system=container_name)
    dir_client = file_system_client.get_directory_client(directory_name)
    dir_client.create_directory()


    #Upload data    
    content= json.dumps(content)
    data=content
    filename=datetime.today().strftime('%Y%m%d%H%M%S%f')+".json"

    file_client = dir_client.get_file_client(filename)
    file_client.upload_data(data, overwrite=True)


# COMMAND ----------

class Dataprinter(tweepy.StreamingClient):

    def on_data(self, raw_data):
        
        
        message_time=datetime.today().strftime('%Y%m%d%H%M%S')
        try:
            
            global data
            global data_json
            data=raw_data
            data=data.decode('unicode_escape').encode('latin-1').decode('utf-8')      
            data_load=json.loads(data,strict=False)
            data_json=json.dumps(data_load, ensure_ascii=False)
            try:
                data_json=data_json.encode('latin-1').decode('unicode_escape').encode('latin-1').decode('utf-8')
            except:
                data_json=data_json

            name_json=message_time+".json"   
        
        
#             Save json
#             with open(name_json, 'w', encoding='utf-8') as f:
#                 json.dump(data_json, f,ensure_ascii=False)   
                
#            Send to event hub   
            data_json
            send_event(data_json)
    
#             Send to ADLS
#            upload_adls_file(data_json)
    
                
        except Exception as e:
            print(str(e))

        
token="XXXXXX"
Dataprinter = Dataprinter(token)

# COMMAND ----------

Dataprinter.get_rules()

# COMMAND ----------

rule="from:use_mas OR from:marca OR from:fifaworldcup_es OR from:FIFAWorldCup"
Dataprinter.add_rules(tweepy.StreamRule(rule)

# COMMAND ----------

Dataprinter.filter(tweet_fields=["id","text","author_id","created_at","geo","lang","in_reply_to_user_id","referenced_tweets","source"])

# COMMAND ----------

Dataprinter.delete_rules(ids='594420042988494849')
