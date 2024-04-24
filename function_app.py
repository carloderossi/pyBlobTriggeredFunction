import azure.functions as func
import logging
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

app = func.FunctionApp()

def get_names(name:str):
    container_name= name.split('/')[0]
    blob_name = name.split('/')[1]
    return container_name, blob_name

@app.blob_trigger(arg_name="myblob", path="certin",
                               connection="connettimi") 
def blob_trigger(myblob: func.InputStream):
    container_name = None
    blob_name = None
    destination_container_name = 'certout'
    stg_acc_url = None
    try:
        logging.info(f"Python blob trigger function processed blob:"
                f"\n\tName: {myblob.name}"
                f"\n\tBlob Size: {myblob.length} bytes")
        logging.info(f"\n\tURI: {myblob.uri}")
        container_name, blob_name = get_names(myblob.name)
        logging.info(f"\n\tblob_name: {blob_name}")
        logging.info(f"\n\tcontainer_name: {container_name}")
        myblob.close()
        stg_acc_url = str(myblob.uri).split('.')[0]
    except Exception as e: 
        logging.exception(f"Blob trigger function failed: {e}")
    try:
        logging.info(f"\n\tFUNC: {func}")
    except Exception as e: 
        logging.exception(f"Blob trigger function failed: {e}")
    try:
        logging.info(f"\n\tAPP: {app}")
    except Exception as e: 
        logging.exception(f"Blob trigger function failed: {e}")
    try:
        connection_string = os.getenv('connettimi')
        logging.info(f"CONN: {connection_string}")
        
        # Create a BlobServiceClient for the Storage Account
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Create a BlobClient for the source blob
        source_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        # Read the content of the source blob
        source_blob_content = source_blob_client.download_blob().readall()
        
        # Create a BlobClient for the destination blob
        destination_blob_client = blob_service_client.get_blob_client(container=destination_container_name, blob=blob_name)
        
        # Upload the content to the destination blob
        destination_blob_client.upload_blob(source_blob_content, overwrite=True)
    
        logging.info(f"\n\tBlob {blob_name} copied from container {container_name} to container {destination_container_name}")
        logging.info(f"\n\t{stg_acc_url}.blob.core.windows.net/{destination_container_name}/{blob_name}")
    except Exception as e: 
        logging.exception(f"Blob trigger function failed: {e}")
        
    