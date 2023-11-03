import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    day_to_backfill = context.get_input()["day_to_backfill"]
    blob_names = context.get_input()["blob_names"]

    staging_table_name = yield context.call_activity(
        "CreateStagingTable", {"day_to_backfill": day_to_backfill}
    )

    tasks = [
        context.call_activity(
            "UploadToStagingTable",
            # retry_options=retry_once_a_minute_three_times,
            input_={"staging_table_name": staging_table_name, "blob_name": blob_name},
        )
        for blob_name in blob_names
    ]
    yield context.task_all(tasks)

    yield context.call_activity(
        "DecompressBackfill",
        input_={
            "staging_table_name": staging_table_name,
            "day_to_backfill": day_to_backfill,
        },
    )

    return "Success"


main = df.Orchestrator.create(orchestrator_function)
