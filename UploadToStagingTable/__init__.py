import logging
import os

import asyncpg
import pandas as pd
from azure.storage.blob.aio import BlobServiceClient

from ParseJsons.load_data import download_blob_into_stream
from ParseJsons.timescale_client import TimeScaleClient


async def main(inputParameters: str) -> str:
    logging.info(f"Running with {inputParameters=}")
    target_connection_string = os.getenv("AzureWebJobsStorage")

    blob_service_client = BlobServiceClient.from_connection_string(
        target_connection_string
    )
    container_client = blob_service_client.get_container_client(
        container="backfill"
    )
    raw_parquet = await download_blob_into_stream(inputParameters["blob_name"], container_client)

    df = pd.read_parquet(raw_parquet)
    df = prepare_dataframe(df)


    password = os.getenv("TIMESCALE_PASSWORD")
    username = os.getenv("TIMESCALE_USERNAME")
    host = os.getenv("TIMESCALE_HOST_URL")
    port = os.getenv("TIMESCALE_PORT")
    dbname = os.getenv("TIMESCALE_DATABASE_NAME")

    conn = await asyncpg.connect(
        f"postgres://{username}:{password}@{host}:{port}/{dbname}"
    )

    timescale_client = TimeScaleClient(connection=conn)
    await timescale_client.copy_many_to_table(table_name=inputParameters["staging_table_name"], data=list(df.itertuples(index=False, name=None)))
    await conn.close()
    return "Success"


def prepare_dataframe(df: pd.DataFrame):
    df = df[["ts", "signal_id", "value"]]
    return df

