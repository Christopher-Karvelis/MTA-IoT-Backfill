import datetime

import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    user_input = context.get_input()

    yield context.call_activity("InitializeSignalHashTable", input_=user_input)

    chunked_timespan = chunk_timespan(user_input)
    retry_once_a_minute_three_times = df.RetryOptions(60_000, 3)

    json_to_parquet_tasks = [
        context.call_activity(
            "ParseJsons",
            # retry_options=retry_once_a_minute_three_times,
            input_={**user_input, **timespan},
        )
        for timespan in chunked_timespan
    ]
    blobs_to_consider = yield context.task_all(json_to_parquet_tasks)

    days_and_backfill_information = produce_grouped_and_filtered_inputs(
        user_input, blobs_to_consider
    )

    backfill_tasks = [
        context.call_sub_orchestrator("BackfillOrchestrator", day_and_blobs)
        for day_and_blobs in days_and_backfill_information
    ]

    yield context.task_all(backfill_tasks)

    return "Success"


main = df.Orchestrator.create(orchestrator_function)


def chunk_timespan(
    timespan,
    chunk_size_in_hours=1,
):
    datetime_from, datetime_to = _convert_timespan_to_datetimes(timespan)
    interval = datetime.timedelta(hours=chunk_size_in_hours)
    periods = []
    period_start = datetime_from
    while period_start < datetime_to:
        period_end = min(period_start + interval, datetime_to)
        periods.append(
            {"ts_start": period_start.isoformat(), "ts_end": period_end.isoformat()}
        )
        period_start = period_end
    return periods


def produce_grouped_and_filtered_inputs(timespan, blobs_to_consider):
    datetime_from, datetime_to = _convert_timespan_to_datetimes(timespan)
    blobs_to_consider_flat = [
        blob_name for blobs in blobs_to_consider for blob_name in blobs
    ]
    date_parts = set(
        date_part.split("/_from_")[0] for date_part in blobs_to_consider_flat
    )
    date_parts_within_timespan = [
        day
        for day in date_parts
        if (datetime_from - datetime.timedelta(days=1))
        <= datetime.datetime.fromisoformat(day)
        <= datetime_to
    ]
    return [
        {
            "day_to_backfill": date_part,
            "blob_names": [
                item for item in blobs_to_consider_flat if item.startswith(date_part)
            ],
        }
        for date_part in date_parts_within_timespan
    ]


def _convert_timespan_to_datetimes(timespan):
    datetime_from = datetime.datetime.fromisoformat(timespan["ts_start"])
    datetime_to = datetime.datetime.fromisoformat(timespan["ts_end"])
    return datetime_from, datetime_to
