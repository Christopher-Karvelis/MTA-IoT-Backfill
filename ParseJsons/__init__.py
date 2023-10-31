import json
import logging
import os

from ParseJsons.move_blobs_to_backfill_container import move_blobs
import dask.dataframe as dd


def main(inputParameters: dict) -> str:
    logging.info(f"Running with {inputParameters=}")

    storage_options = {"connection_string": os.getenv("SOURCE_STORAGE_ACCOUNT_CONNECTION_STRING")}

    target_connection_string = os.getenv("AzureWebJobsStorage")
    target_storage_options = {"connection_string": target_connection_string}

    with open("signal_dictionary.json", "r") as f:
        signal_hash_table = json.load(f)


    parsed_input = inputParameters['ts_start'].split('T')
    date = parsed_input[0]
    hour = parsed_input[1][:2]
    pattern_to_read = f"abfs://axh-opcpublisher/{date}/{hour}/*.json"
    logging.info(f"will try to read {pattern_to_read=}")
    ddf = dd.read_json(pattern_to_read, storage_options=storage_options, lines=False)
    ddf["hash_key"] = ddf["control_system_identifier"] + ddf["plant"]
    ddf["signal_id"] = ddf["hash_key"].map(lambda x: signal_hash_table[x], meta=('signal_id', 'i8'))
    dd.to_parquet(ddf, path=f"abfs://backfill/{date}T{hour}", storage_options=target_storage_options)
    print(f"read for {pattern_to_read=} {len(ddf)=}")
    print(f"read for {pattern_to_read=} {ddf.head()=}")

    #dd.to_sql(ddf, "_hack_backfill", if_exists="append", uri=)
    return "Success"

