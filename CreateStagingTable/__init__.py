from shared.timescale_client import TimeScaleClient


async def main(inputParameters: dict) -> str:
    timescale_client = TimeScaleClient.from_env_vars()
    async with timescale_client.connect():
        staging_table_name = await timescale_client.create_staging_table(
            inputParameters["day_to_backfill"]
        )
    return staging_table_name
