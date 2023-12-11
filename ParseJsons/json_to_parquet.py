import json
import asyncio
import logging
import pandas as pd
import azure.functions as func
import azure.durable_functions as df

from typing import List, Dict
from shared_assets import helpers
from shared_assets.input_structure import InputParameters
from ParseJsons.upload_data import upload_grouped_as_parquet
from shared_assets.azure_blob import get_backfilling_container_client
from ParseJsons.download_data import load_one_hour_of_data_starting_at
from signal_hashtable.signal_hash_table import SignalHashTablePersistence




RETRY_ONCE_A_MINUTE_THREE_TIMES = df.RetryOptions(60_000, 3)
bp = df.Blueprint() 



@bp.route(methods=["post"], route="orchestrators/json_to_parquet") 
@bp.durable_client_input(client_name="client")
async def http_json_to_parquet(req: func.HttpRequest, client) -> func.HttpResponse:
    try:
        req_body: Dict = req.get_json()
        input_parameters = InputParameters(**req_body)
        logging.info(input_parameters)
    except ValueError:
        logging.error(f"JSON could not be read from body: {req.get_body().decode()}")
        error_message = "Bad Requ est: Invalides Json."
        return func.HttpResponse(json.dumps({"error": error_message}), status_code=400)

    instance_id = await client.start_new(
        "json_to_parquet_orchestrator", client_input=input_parameters.dict()
    )

    return client.create_check_status_response(req, instance_id)

@bp.orchestration_trigger(context_name="context")
def json_to_parquet_orchestrator(context: df.DurableOrchestrationContext):
    user_input = context.get_input()
    chunked_timespan = helpers.chunk_timespan(user_input)

    json_to_parquet_tasks = [
        context.call_activity_with_retry(
            "parse_jsons",
            retry_options=RETRY_ONCE_A_MINUTE_THREE_TIMES,
            input_={**user_input, **timespan},
        )
        for timespan in chunked_timespan
    ]
    blobs_to_consider = yield context.task_all(json_to_parquet_tasks)
    return blobs_to_consider

@bp.activity_trigger(input_name="inputParameters")
async def parse_jsons(inputParameters: dict) -> List[str]:
    container_client = get_backfilling_container_client()
    signal_hash_table_persistence = SignalHashTablePersistence(container_client)
    signal_hash_table = await signal_hash_table_persistence.download_signal_hash_table()

    parsed_input = inputParameters["ts_start"].split("T")
    raw_data, read_from = await load_one_hour_of_data_starting_at(
        date=parsed_input[0], hour=parsed_input[1][:2]
    )

    df = helpers.turn_result_into_dataframe(raw_data)

    event_loop = asyncio.get_event_loop()
    df = await event_loop.run_in_executor(None, helpers.prepare_signal_dataframe, df, signal_hash_table)

    # this is a hack to be really really really sure that it is all numeric
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    logging.info(f"Hiii {df.dtypes=}")


    blobs_to_consider = await upload_grouped_as_parquet(container_client, df, read_from)

    return blobs_to_consider
