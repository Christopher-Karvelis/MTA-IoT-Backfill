import os
import json

from azure.storage.blob import BlobServiceClient

from InitializeSignalHashTable.signal_client import SignalClient


def main(inputParameters: dict) -> str:
    target_connection_string = os.getenv("AzureWebJobsStorage")

    signal_client = SignalClient()
    hash_table = signal_client.provide_hash_table()

    blob_service_client = BlobServiceClient.from_connection_string(
        target_connection_string
    )
    container_client = blob_service_client.get_container_client(
        container="backfill"
    )
    blob_name = f'signal_hash_table'
    blob_client = container_client.get_blob_client(blob=blob_name)

    data = json.dumps(hash_table)

    blob_client.upload_blob(data)

    return "Success"
