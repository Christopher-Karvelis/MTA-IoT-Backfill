import os
import json

import asyncpg
from azure.storage.blob import BlobServiceClient

from InitializeSignalHashTable.signal_client import SignalClient
# this shouldn't be done, it should be imported from shared
from ParseJsons import TimeScaleClient


async def main(inputParameters: dict) -> str:
    target_connection_string = os.getenv("AzureWebJobsStorage")

    signal_client = SignalClient()
    hash_table = signal_client.provide_hash_table()

    blob_service_client = BlobServiceClient.from_connection_string(
     target_connection_string
    )
    container_client = blob_service_client.get_container_client(
     container="backfill"
    )
    blob_name = "signal_hash_table"
    blob_client = container_client.get_blob_client(blob=blob_name)

    data = json.dumps(hash_table)

    blob_client.upload_blob(data, overwrite=True)
    return "wat"
