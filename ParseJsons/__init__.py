import asyncio
import io
import json
import logging
import os

import pandas as pd
from azure.storage.blob.aio import BlobServiceClient

from ParseJsons.move_blobs_to_backfill_container import move_blobs


async def main(inputParameters: dict) -> str:
    logging.info(f"Running with {inputParameters=}")

    storage_options = {"connection_string": os.getenv("SOURCE_STORAGE_ACCOUNT_CONNECTION_STRING")}

    #target_connection_string = os.getenv("AzureWebJobsStorage")
    #target_storage_options = {"connection_string": target_connection_string}

    with open("signal_dictionary.json", "r") as f:
        signal_hash_table = json.load(f)

    parsed_input = inputParameters['ts_start'].split('T')
    date = parsed_input[0]
    hour = parsed_input[1][:2]
    # put axh-opcpublisher instead of backfill for real stuff
    pattern_to_read = f"{date}/{hour}"
    logging.info(f"will try to read {pattern_to_read=}")

    result = asyncio.new_event_loop().run_until_complete(get_data(storage_options, pattern_to_read))

    df = turn_result_into_dataframe(result)

    df = prepare_dataframe(df, signal_hash_table)
    # todo: filter not a number
    # todo: filter time

    password = os.getenv("TIMESCALE_PASSWORD")
    username = os.getenv("TIMESCALE_USERNAME")
    host = os.getenv("TIMESCALE_HOST_URL")
    port = os.getenv("TIMESCALE_PORT")
    dbname = os.getenv("TIMESCALE_DATABASE_NAME")

    # print(f"ready to upload {pattern_to_read=}")
    uri = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
    df.to_sql("_hack_backfill", if_exists="append", index=False, uri=uri, method=postgres_upsert)#, parallel=True)
    return "Success"


def prepare_dataframe(df: pd.DataFrame, signal_hash_table: dict):
    df["hash_key"] = df["control_system_identifier"] + df["plant"]
    df["signal_id"] = df["hash_key"].map(lambda x: signal_hash_table[x])
    df = df.rename(columns={"measurement_value": "value"})
    df = df[["signal_id", "ts", "value"]]
    return df


def turn_result_into_dataframe(result_strings):
    return pd.concat([pd.read_json(result_string) for result_string in result_strings])

async def get_data(storage_options, pattern_to_read):
    blob_service_client = BlobServiceClient.from_connection_string(
        storage_options["connection_string"]
    )
    source_container_client = blob_service_client.get_container_client("backfill")

    blobs_with_start_to_consider = source_container_client.list_blobs(name_starts_with=pattern_to_read)
    tasks = [download_blob(blob, source_container_client) async for blob in blobs_with_start_to_consider]
    results = await asyncio.gather(*tasks)
    return results

async def download_blob(blob, container_client):
    blob_client = container_client.get_blob_client(blob["name"])

    blob_response = await blob_client.download_blob()
    blob_content = await blob_response.readall()
    return io.StringIO(blob_content.decode('utf-8'))


def postgres_upsert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_nothing()
    conn.execute(upsert_statement)
