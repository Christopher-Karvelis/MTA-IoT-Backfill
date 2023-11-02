import os

import asyncpg

# this shouldn't be done, it should be imported from shared
from ParseJsons.timescale_client import TimeScaleClient


async def main(inputParameters: dict) -> str:
    password = os.getenv("TIMESCALE_PASSWORD")
    username = os.getenv("TIMESCALE_USERNAME")
    host = os.getenv("TIMESCALE_HOST_URL")
    port = os.getenv("TIMESCALE_PORT")
    dbname = os.getenv("TIMESCALE_DATABASE_NAME")

    conn = await asyncpg.connect(
        f"postgres://{username}:{password}@{host}:{port}/{dbname}"
    )

    staging_table_name = (
        f"_backfill_{inputParameters['day_to_backfill']}".replace("-", "_")
        .replace(":", "_")
        .replace("+", "_")
        .replace(":", "_")
        .replace("T", "_")
    )
    timescale_client = TimeScaleClient(connection=conn)
    await timescale_client.create_staging_table(staging_table_name)

    return staging_table_name
