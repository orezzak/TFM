import logging
import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient
import json
from datetime import datetime

def get_adls_details():   

    # Get the below details from your storage account
    storage_account_name = "datalakegen2tfm"
    storage_account_key = "xxxxx"
    

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
    #content= json.dumps(content)
    data=content
    filename=datetime.today().strftime('%Y%m%d%H%M%S%f')+".json"

    file_client = dir_client.get_file_client(filename)
    file_client.upload_data(data, overwrite=True)


def main(events: func.EventHubEvent):
    try:
        for event in events:
            logging.info('Python EventHub trigger processed an event: %s',
                        event.get_body().decode('utf-8'))
        print(event.get_body().decode('utf-8'))

        upload_adls_file(event.get_body().decode('utf-8'))
    except Exception as e:        
        logging.info(str(e))
