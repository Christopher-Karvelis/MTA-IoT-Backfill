import json
import logging
import azure.functions as func
from typing import Dict
import azure.durable_functions as df
from signal_client import SignalClient
from utils.azure_blob import get_backfilling_container_client
from utils.signal_hash_table import SignalHashTablePersistence



app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.activity_trigger(input_name="inputParameters")
async def initialize_signal_hashtable(inputParameters: dict) -> str:
    # it is not ideal to block like this in an async function
    hash_table = SignalClient().provide_hash_table()

    container_client = get_backfilling_container_client()
    signal_hash_table_persistence = SignalHashTablePersistence(container_client)
    await signal_hash_table_persistence.upload_signal_hash_table(hash_table)
    return "Success"



@app.orchestration_trigger(context_name="context")
def json_to_parquet_orchestrator(context: df.DurableOrchestrationContext):
    user_input = context.get_input()
    retry_once_a_minute_three_times = df.RetryOptions(60_000, 3)

    yield context.call_activity_with_retry("initialize_signal_hashtable",
                                           input_=user_input,
                                           retry_options=retry_once_a_minute_three_times)
    return "Success"



@app.route(methods=["post"], route="orchestrators/{functionName}") 
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client) -> func.HttpResponse:
    try:
        req_body: Dict = req.get_json()
        input_parameters = req_body
        logging.info(input_parameters)
    except ValueError:
        logging.error(f"JSON could not be read from body: {req.get_body().decode()}")
        error_message = "Bad Requ est: Invalides Json."
        return func.HttpResponse(json.dumps({"error": error_message}), status_code=400)

    instance_id = await client.start_new(
        "json_to_parquet_orchestrator", client_input=input_parameters.dict()
    )

    return client.create_check_status_response(req, instance_id)
