import json
import logging
import os

import asyncpg
import pandas as pd
from azure.storage.blob.aio import BlobServiceClient

from ParseJsons.load_data import load_one_hour_of_data_starting_at, download_blob
from ParseJsons.move_blobs_to_backfill_container import move_blobs
from ParseJsons.timescale_client import TimeScaleClient


async def main(inputParameters: dict) -> str:
    logging.info(f"Running with {inputParameters=}")
    target_connection_string = os.getenv("AzureWebJobsStorage")

    blob_service_client = BlobServiceClient.from_connection_string(
        target_connection_string
    )
    container_client = blob_service_client.get_container_client(
        container="backfill"
    )
    blob_name = f'signal_hash_table'
    signal_hash_table = json.load(await download_blob(blob_name, container_client))

    #this is kind of a hack. better: turn the string into a datetime before hand.
    # Then extract the information in the loading function
    parsed_input = inputParameters['ts_start'].split('T')
    raw_data = await load_one_hour_of_data_starting_at(date=parsed_input[0], hour=parsed_input[1][:2])

    df = turn_result_into_dataframe(raw_data)

    # this is blocking and takes a long time, that is no bueno for async
    df = prepare_dataframe(df, signal_hash_table)


    password = os.getenv("TIMESCALE_PASSWORD")
    username = os.getenv("TIMESCALE_USERNAME")
    host = os.getenv("TIMESCALE_HOST_URL")
    port = os.getenv("TIMESCALE_PORT")
    dbname = os.getenv("TIMESCALE_DATABASE_NAME")

    conn = await asyncpg.connect(
        f"postgres://{username}:{password}@{host}:{port}/{dbname}"
    )

    timescale_client = TimeScaleClient(connection=conn)
    await timescale_client.copy_many_to_table(table_name="_hack_backfill", data=list(df.itertuples(index=False, name=None)))

    return "Success"


def prepare_dataframe(df: pd.DataFrame, signal_hash_table: dict):
    # todo: filter not a number
    # todo: filter time
    df["hash_key"] = df["control_system_identifier"] + df["plant"]
    df["signal_id"] = df["hash_key"].map(lambda x: signal_hash_table[x])
    df = df.rename(columns={"measurement_value": "value"})
    df["ts"] = pd.to_datetime(df["ts"])
    df = df[["ts", "signal_id", "value"]]
    return df


def turn_result_into_dataframe(result_strings):
    return pd.concat([pd.read_json(result_string) for result_string in result_strings])

