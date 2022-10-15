from azure.storage.blob import *
from datetime import date
import os

def get_all_planner_data(account_name, account_key, container_name, output_path):
    """
    Download file from storage blob azure
    """
    print("Getting data from planner")
    today = date.today().strftime("%d-%m-%Y")
    # Create the BlockBlockService that the system uses to call the Blob service for the storage account.
    block_blob_service = BlockBlobService(
        account_name=account_name, account_key=account_key)

    # Dowload all blobs in container
    for blob in block_blob_service.list_blobs(container_name):
        if today in blob.name:
            file_name = blob.name[:-16] + blob.name[-5:] 
            local_file_path = os.path.join(output_path, file_name)
            block_blob_service.get_blob_to_path(
                container_name, blob.name, local_file_path)

            print('     + Dowloaded successfull: ', os.path.abspath(local_file_path))