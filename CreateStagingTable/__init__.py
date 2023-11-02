from shared.timescale_client import TimeScaleClient


async def main(inputParameters: dict) -> str:
    # is is possible to do something with a contextmanager here so we close the connection always?
    timescale_client = await TimeScaleClient.from_env_vars()
    staging_table_name = await timescale_client.create_staging_table(inputParameters['day_to_backfill'])
    return staging_table_name
