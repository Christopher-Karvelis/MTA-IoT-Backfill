import datetime
import numpy as np
import pandas as pd

def _convert_timespan_to_datetimes(timespan):
    datetime_from = datetime.datetime.fromisoformat(timespan["ts_start"])
    datetime_to = datetime.datetime.fromisoformat(timespan["ts_end"])
    return datetime_from, datetime_to



def produce_grouped_and_filtered_inputs(user_input):
    datetime_from, datetime_to = _convert_timespan_to_datetimes(user_input["timespan"])
    blobs_to_consider_flat = [
        blob_name for blobs in user_input["blobs_to_consider"] for blob_name in blobs
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
    assembled_with_blob_names = [
        {
            "day_to_backfill": date_part,
            "blob_names": [
                item for item in blobs_to_consider_flat if item.startswith(date_part)
            ],
        }
        for date_part in date_parts_within_timespan
    ]
    assembled_with_blob_names.sort(key=lambda x: x["day_to_backfill"])
    return assembled_with_blob_names

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


def prepare_signal_dataframe(df: pd.DataFrame, signal_hash_table: dict):
    df["hash_key"] = df["control_system_identifier"] + df["plant"]
    df["hash_key"] = df["hash_key"].str.replace("KWI-WHA", "KWI")
    df["signal_id"] = df["hash_key"].map(lambda x: signal_hash_table.get(x, np.nan))
    df.drop(columns=["hash_key"], inplace=True)
    df = df.rename(columns={"measurement_value": "value"})
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def prepare_dataframe(df: pd.DataFrame):
    df = df[["ts", "signal_id", "value"]]
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df = df.dropna()
    return df


def turn_result_tuple_into_dataframe(result_tuple):
    df = pd.read_json(result_tuple[1])
    df["source"] = result_tuple[0]
    if len(df) > 0:
        df["measurement_value"] = pd.to_numeric(df["measurement_value"], errors="coerce")
    return df


def turn_result_into_dataframe(result_tuples):
    return pd.concat(
        [turn_result_tuple_into_dataframe(result) for result in result_tuples]
    )
