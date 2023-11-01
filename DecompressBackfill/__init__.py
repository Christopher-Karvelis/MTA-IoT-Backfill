import os
import json

import asyncpg
from azure.storage.blob import BlobServiceClient

from InitializeSignalHashTable.signal_client import SignalClient
# this shouldn't be done, it should be imported from shared
from ParseJsons import TimeScaleClient


async def main(inputParameters: dict) -> str:
    password = os.getenv("TIMESCALE_PASSWORD")
    username = os.getenv("TIMESCALE_USERNAME")
    host = os.getenv("TIMESCALE_HOST_URL")
    port = os.getenv("TIMESCALE_PORT")
    dbname = os.getenv("TIMESCALE_DATABASE_NAME")

    conn = await asyncpg.connect(
        f"postgres://{username}:{password}@{host}:{port}/{dbname}"
    )

    staging_table_name = inputParameters["staging_table_name"]
    timescale_client = TimeScaleClient(connection=conn)
    await timescale_client.decompress_backfill(staging_table_name, "measurements")
    await timescale_client.drop_staging_table(staging_table_name)

    return "SUCCESS"
