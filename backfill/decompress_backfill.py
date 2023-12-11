import json
import logging
import pandas as pd
import azure.functions as func
import azure.durable_functions as df

from typing import Dict
from shared_assets import azure_blob
from shared_assets import helpers
from shared_assets.input_structure import InputParameters
from shared_assets.timescale_client import TimeScaleClient



bp = df.Blueprint() 

@bp.route(methods=["post"], route="orchestrators/backfill") 
@bp.durable_client_input(client_name="client")
async def http_backfill(req: func.HttpRequest, client) -> func.HttpResponse:
    try:
        req_body: Dict = req.get_json()
        input_parameters = InputParameters(**req_body)
        logging.info(input_parameters)
    except ValueError:
        logging.error(f"JSON could not be read from body: {req.get_body().decode()}")
        error_message = "Bad Requ est: Invalides Json."
        return func.HttpResponse(json.dumps({"error": error_message}), status_code=400)

    instance_id = await client.start_new(
        "backfill_orchestrator", client_input=input_parameters.dict()
    )

    return client.create_check_status_response(req, instance_id)


@bp.orchestration_trigger(context_name="context")
def backfill_orchestrator(context: df.DurableOrchestrationContext):
    days_and_backfill_information = helpers.produce_grouped_and_filtered_inputs(
        user_input=context.get_input()
    )

    backfill_tasks = [
        context.call_sub_orchestrator("backfill_sub_orchestrator", day_and_blobs)
        for day_and_blobs in days_and_backfill_information
    ]

    yield context.task_all(backfill_tasks)

    return "Success"


@bp.orchestration_trigger(context_name="context")
def backfill_sub_orchestrator(context: df.DurableOrchestrationContext):
    day_to_backfill = context.get_input()["day_to_backfill"]
    blob_names = context.get_input()["blob_names"]

    staging_table_name = yield context.call_activity(
        "create_staging_table", {"day_to_backfill": day_to_backfill}
    )

    retry_once_a_minute_three_times = df.RetryOptions(60_000, 3)

    tasks = [
        context.call_activity_with_retry(
            "upload_to_staging_table",
            retry_options=retry_once_a_minute_three_times,
            input_={"staging_table_name": staging_table_name, "blob_name": blob_name},
        )
        for blob_name in blob_names
    ]
    yield context.task_all(tasks)

    yield context.call_activity(
        "decompress_backfill",
        input_={
            "staging_table_name": staging_table_name,
            "day_to_backfill": day_to_backfill,
        },
    )

    return "Success"


@bp.activity_trigger(input_name="inputParameters")
async def decompress_backfill(inputParameters: dict) -> str:
    staging_table_name = inputParameters["staging_table_name"]
    day_to_backfill = inputParameters["day_to_backfill"]
    timescale_client = TimeScaleClient.from_env_vars()
    async with timescale_client.connect():
        await timescale_client.decompress_backfill(
            day_to_backfill, staging_table_name, "measurements"
        )
        await timescale_client.drop_staging_table(staging_table_name)

    return "SUCCESS"



@bp.activity_trigger(input_name="inputParameters")
async def create_staging_table(inputParameters: dict) -> str:
    timescale_client = TimeScaleClient.from_env_vars()
    async with timescale_client.connect():
        staging_table_name = await timescale_client.create_staging_table(
            inputParameters["day_to_backfill"]
        )
    return staging_table_name


@bp.activity_trigger(input_name="inputParameters")
async def upload_to_staging_table(inputParameters: dict) -> str:
    container_client = azure_blob.get_backfilling_container_client()
    raw_parquet = await azure_blob.download_blob_into_stream(
        inputParameters["blob_name"], container_client
    )

    df = pd.read_parquet(raw_parquet)
    df = helpers.prepare_dataframe(df)

    timescale_client = TimeScaleClient.from_env_vars()
    await timescale_client.hack_unclosing_connect()
    await timescale_client.copy_many_to_table(
        table_name=inputParameters["staging_table_name"],
        data=list(df.itertuples(index=False, name=None)),
    )
    return "Success"
