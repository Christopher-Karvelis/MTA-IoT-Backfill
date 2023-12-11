import azure.functions as func
import azure.durable_functions as df
from signal_hashtable.initialize_signal_hashtable import bp
from ParseJsons.json_to_parquet import bp as bp2


RETRY_ONCE_A_MINUTE_THREE_TIMES = df.RetryOptions(60_000, 3)
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)
app.register_functions(bp)
app.register_functions(bp2)  

# @app.activity_trigger(input_name="inputParameters")
# async def initialize_signal_hashtable(inputParameters: dict) -> str:
#     # it is not ideal to block like this in an async function
#     hash_table = SignalClient().provide_hash_table()

#     container_client = get_backfilling_container_client()
#     signal_hash_table_persistence = SignalHashTablePersistence(container_client)
#     await signal_hash_table_persistence.upload_signal_hash_table(hash_table)
#     return "Success"


# @app.orchestration_trigger(context_name="context")
# def json_to_parquet_orchestrator(context: df.DurableOrchestrationContext):
#     user_input = context.get_input()
#     chunked_timespan = chunk_timespan(user_input)

#     json_to_parquet_tasks = [
#         context.call_activity_with_retry(
#             "ParseJsons",
#             retry_options=RETRY_ONCE_A_MINUTE_THREE_TIMES,
#             input_={**user_input, **timespan},
#         )
#         for timespan in chunked_timespan
#     ]
#     blobs_to_consider = yield context.task_all(json_to_parquet_tasks)
#     days_and_backfill_information = produce_grouped_and_filtered_inputs(
#         user_input, blobs_to_consider
#     )

#     backfill_tasks = [
#         context.call_sub_orchestrator("BackfillOrchestrator", day_and_blobs)
#         for day_and_blobs in days_and_backfill_information
#     ]

#     yield context.task_all(backfill_tasks)
    
#     return "Success"


# @app.route(methods=["post"], route="orchestrators/initialize_signal_hashtable") 
# @app.durable_client_input(client_name="client")
# async def http_start_signal_hashtable(req: func.HttpRequest, client) -> func.HttpResponse:
#     try:
#         req_body: Dict = req.get_json()
#         input_parameters = InputParameters(**req_body)
#         logging.info(input_parameters)
#     except ValueError:
#         logging.error(f"JSON could not be read from body: {req.get_body().decode()}")
#         error_message = "Bad Requ est: Invalides Json."
#         return func.HttpResponse(json.dumps({"error": error_message}), status_code=400)

#     instance_id = await client.start_new(
#         "initialize_signal_hashtable_orchestrator", client_input=input_parameters.dict()
#     )

#     return client.create_check_status_response(req, instance_id)