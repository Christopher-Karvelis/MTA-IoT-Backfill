from shared.timescale_client import TimeScaleClient


async def main(inputParameters: dict) -> str:
    staging_table_name = inputParameters["staging_table_name"]
    day_to_backfill = inputParameters["day_to_backfill"]
    timescale_client = TimeScaleClient.from_env_vars()
    await timescale_client.connect()
    await timescale_client.decompress_backfill(
        day_to_backfill, staging_table_name, "measurements"
    )
    await timescale_client.drop_staging_table(staging_table_name)

    return "SUCCESS"
