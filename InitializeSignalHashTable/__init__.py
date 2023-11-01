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

    password = os.getenv("TIMESCALE_PASSWORD")
    username = os.getenv("TIMESCALE_USERNAME")
    host = os.getenv("TIMESCALE_HOST_URL")
    port = os.getenv("TIMESCALE_PORT")
    dbname = os.getenv("TIMESCALE_DATABASE_NAME")

    conn = await asyncpg.connect(
        f"postgres://{username}:{password}@{host}:{port}/{dbname}"
    )

    # I should probably parse the datetime to protect against sql injection...
    staging_table_name = f"_backfill_{inputParameters['ts_start']}".replace("-", "_").replace(":", "_").replace("+", "_").replace(":","_").replace("T", "_")
    timescale_client = TimeScaleClient(connection=conn)
    await timescale_client.create_staging_table(staging_table_name)

    return staging_table_name
