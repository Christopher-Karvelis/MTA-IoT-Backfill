from typing import List

import pandas as pd

from ParseJsons.download_data import load_one_hour_of_data_starting_at

from ParseJsons.upload_data import upload_grouped_as_parquet
from utils.azure_blob import get_backfilling_container_client
from utils.signal_hash_table import SignalHashTablePersistence


async def main(inputParameters: dict) -> List[str]:
    container_client = get_backfilling_container_client()
    signal_hash_table_persistence = SignalHashTablePersistence(container_client)
    signal_hash_table = await signal_hash_table_persistence.download_signal_hash_table()

    parsed_input = inputParameters["ts_start"].split("T")
    raw_data, read_from = await load_one_hour_of_data_starting_at(
        date=parsed_input[0], hour=parsed_input[1][:2]
    )

    df = turn_result_into_dataframe(raw_data)

    # this is blocking and takes a long time, that is no bueno for async
    df = prepare_dataframe(df, signal_hash_table)

    blobs_to_consider = await upload_grouped_as_parquet(container_client, df, read_from)

    return blobs_to_consider


def prepare_dataframe(df: pd.DataFrame, signal_hash_table: dict):
    df["hash_key"] = df["control_system_identifier"] + df["plant"]
    df["hash_key"] = df["hash_key"].str.replace("KWI-WHA", "KWI")
    df["signal_id"] = df["hash_key"].map(lambda x: signal_hash_table[x])
    df.drop(columns=["hash_key"], inplace=True)
    df = df.rename(columns={"measurement_value": "value"})
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def turn_result_tuple_into_dataframe(result_tuple):
    # this feels hacky because it is
    df = pd.read_json(result_tuple[1])
    df["source"] = result_tuple[0]
    return df


def turn_result_into_dataframe(result_tuples):
    return pd.concat(
        [turn_result_tuple_into_dataframe(result) for result in result_tuples]
    )
