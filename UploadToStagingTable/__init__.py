import pandas as pd

from utils.azure_blob import download_blob_into_stream, get_backfilling_container_client
from utils.timescale_client import TimeScaleClient


async def main(inputParameters: dict) -> str:
    container_client = get_backfilling_container_client()
    raw_parquet = await download_blob_into_stream(
        inputParameters["blob_name"], container_client
    )

    df = pd.read_parquet(raw_parquet)
    df = prepare_dataframe(df)

    timescale_client = TimeScaleClient.from_env_vars()
    async with timescale_client.connect():
        await timescale_client.copy_many_to_table(
            table_name=inputParameters["staging_table_name"],
            data=list(df.itertuples(index=False, name=None)),
        )
    return "Success"


def prepare_dataframe(df: pd.DataFrame):
    df = df[["ts", "signal_id", "value"]]
    df = df.dropna()
    return df
