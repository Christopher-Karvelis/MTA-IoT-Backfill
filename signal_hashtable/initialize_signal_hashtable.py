import json
import logging
import azure.functions as func
import azure.durable_functions as df

from typing import Dict
from signal_client import SignalClient
from shared_assets.input_structure import InputParameters
from shared_assets.azure_blob import get_backfilling_container_client
from signal_hashtable.signal_hash_table import SignalHashTablePersistence


bp = df.Blueprint() 


RETRY_ONCE_A_MINUTE_THREE_TIMES = df.RetryOptions(60_000, 3)

@bp.activity_trigger(input_name="inputParameters")
async def initialize_signal_hashtable(inputParameters: dict) -> str:
    # it is not ideal to block like this in an async function
    hash_table = SignalClient().provide_hash_table()

    container_client = get_backfilling_container_client()
    signal_hash_table_persistence = SignalHashTablePersistence(container_client)
    await signal_hash_table_persistence.upload_signal_hash_table(hash_table)
    return "Success"



@bp.orchestration_trigger(context_name="context")
def initialize_signal_hashtable_orchestrator(context: df.DurableOrchestrationContext):
    user_input = context.get_input()

    yield context.call_activity_with_retry("initialize_signal_hashtable",
                                           input_=user_input,
                                           retry_options=RETRY_ONCE_A_MINUTE_THREE_TIMES)
    return "Success"


@bp.route(methods=["post"], route="orchestrators/initialize_signal_hashtable") 
@bp.durable_client_input(client_name="client")
async def http_start_signal_hashtable(req: func.HttpRequest, client) -> func.HttpResponse:
    try:
        req_body: Dict = req.get_json()
        input_parameters = InputParameters(**req_body)
        logging.info(input_parameters)
    except ValueError:
        logging.error(f"JSON could not be read from body: {req.get_body().decode()}")
        error_message = "Bad Requ est: Invalides Json."
        return func.HttpResponse(json.dumps({"error": error_message}), status_code=400)

    instance_id = await client.start_new(
        "initialize_signal_hashtable_orchestrator", client_input=input_parameters.dict()
    )

    return client.create_check_status_response(req, instance_id)