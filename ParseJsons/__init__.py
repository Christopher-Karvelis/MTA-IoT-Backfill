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
    #put axh-opcpublisher instead of backfill for real stuff
    pattern_to_read = f"abfs://backfill/{date}/{hour}/*.json"
    logging.info(f"will try to read {pattern_to_read=}")
    ddf = dd.read_json(pattern_to_read, storage_options=storage_options, lines=False)
    ddf = ddf.repartition(npartitions=10)
    # I am confused why this log takes so long to appear even without compute. are we loading everything into memory?
    #ddf = ddf.compute()
    logging.info(f"No I have all the data for {pattern_to_read=}")
    ddf["hash_key"] = ddf["control_system_identifier"] + ddf["plant"]
    ddf["signal_id"] = ddf["hash_key"].map(lambda x: signal_hash_table[x], meta=('signal_id', 'i8'))
    ddf = ddf.rename(columns={"measurement_value": "value"})
    dd.to_parquet(ddf, path=f"abfs://backfill/{date}T{hour}", storage_options=target_storage_options)

    ddf = ddf[["signal_id", "ts", "value"]]
    # todo: filter not a number
    # todo: filter time

    password = os.getenv("TIMESCALE_PASSWORD")
    username = os.getenv("TIMESCALE_USERNAME")
    host = os.getenv("TIMESCALE_HOST_URL")
    port = os.getenv("TIMESCALE_PORT")
    dbname = os.getenv("TIMESCALE_DATABASE_NAME")


    #print(f"ready to upload {pattern_to_read=}")
    uri = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
    dd.to_sql(ddf, "_hack_backfill", if_exists="append", index=False, uri=uri, method=postgres_upsert, parallel=True)
    return "Success"


def postgres_upsert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_nothing()
    conn.execute(upsert_statement)

