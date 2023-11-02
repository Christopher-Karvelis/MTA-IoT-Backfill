import azure.durable_functions as df

import datetime

import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    user_input = context.get_input()

    yield context.call_activity("InitializeSignalHashTable", input_=user_input)

    chunked_timespan = chunk_timespan(user_input)
    retry_once_a_minute_three_times = df.RetryOptions(60_000, 3)

    tasks = [
        context.call_activity(
            "ParseJsons",
            # retry_options=retry_once_a_minute_three_times,
            input_={**user_input, **timespan},
        )
        for timespan in chunked_timespan
    ]
    blobs_to_consider = yield context.task_all(tasks)

    blobs_to_consider_flat = [blob_name for blobs in blobs_to_consider for blob_name in blobs if
                              blob_name.startswith("2023-10-24")]
    # these should be grouped by the date obviously and then the suborchestrator should be called multiple times

    tasks_2 = [
        context.call_sub_orchestrator("BackfillOrchestrator", blobs_to_consider_flat)
    ]

    yield context.task_all(tasks_2)


    return "Success"


main = df.Orchestrator.create(orchestrator_function)


def chunk_timespan(
        timespan,
        chunk_size_in_hours=1,
):
    datetime_from = datetime.datetime.fromisoformat(timespan["ts_start"])
    datetime_to = datetime.datetime.fromisoformat(timespan["ts_end"])
    interval = datetime.timedelta(hours=chunk_size_in_hours)
    periods = []
    period_start = datetime_from
    while period_start < datetime_to:
        period_end = min(period_start + interval, datetime_to)
        periods.append({"ts_start": period_start.isoformat(), "ts_end": period_end.isoformat()})
        period_start = period_end
    return periods
