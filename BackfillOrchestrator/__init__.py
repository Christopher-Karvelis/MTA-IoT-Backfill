import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    user_input = context.get_input()

    # get the list and then do something with it.....

    staging_table_name = yield context.call_activity("CreateStagingTable", input_=user_input)

    tasks = [
        context.call_activity(
            "UploadToStagingTable",
            # retry_options=retry_once_a_minute_three_times,
            input_={"staging_table_name": staging_table_name, "blob_name": blob_name},
        )
        for blob_name in user_input
    ]
    yield context.task_all(tasks)

    yield context.call_activity(
        "DecompressBackfill", input_={"staging_table_name": staging_table_name}
    )

    return "Success"


main = df.Orchestrator.create(orchestrator_function)

