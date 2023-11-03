import datetime
import os
from contextlib import asynccontextmanager

import asyncpg

DECOMPRESS_BACKFILL_ADVISORY_LOCK = 345678


def _produce_day_after_day_to_backfill(day_to_backfill):
    return (
        datetime.date.fromisoformat(day_to_backfill) + datetime.timedelta(days=1)
    ).isoformat()


def _produce_staging_table_name(day_to_backfill):
    return (
        f"_backfill_{day_to_backfill}".replace("-", "_")
        .replace(":", "_")
        .replace("+", "_")
        .replace(":", "_")
        .replace("T", "_")
    )


class TimeScaleClient:
    def __init__(self, uri):
        self.uri = uri
        self.connection = None

    @classmethod
    def from_env_vars(cls):
        password = os.getenv("TIMESCALE_PASSWORD")
        username = os.getenv("TIMESCALE_USERNAME")
        host = os.getenv("TIMESCALE_HOST_URL")
        port = os.getenv("TIMESCALE_PORT")
        dbname = os.getenv("TIMESCALE_DATABASE_NAME")
        uri = f"postgres://{username}:{password}@{host}:{port}/{dbname}"
        return cls(uri)

    @asynccontextmanager
    async def connect(self):
        try:
            self.connection = await asyncpg.connect(self.uri)
            yield
        finally:
            await self.connection.close()

    async def create_staging_table(self, day_to_backfill):
        day_after_day_to_backfill = _produce_day_after_day_to_backfill(day_to_backfill)
        staging_table_name = _produce_staging_table_name(day_to_backfill)
        await self.connection.execute(
            f"""create table IF NOT EXISTS {staging_table_name} (like measurements excluding indexes excluding constraints,
             constraint time_range_check CHECK (ts >= '{day_to_backfill}' and ts < '{day_after_day_to_backfill}'))"""
        )
        return staging_table_name

    async def copy_many_to_table(self, data, table_name):
        await self.connection.copy_records_to_table(table_name=table_name, records=data)

    async def decompress_backfill(
        self, day_to_backfill, staging_table_name, destination_table_name
    ):
        day_after_day_to_backfill = _produce_day_after_day_to_backfill(day_to_backfill)
        # make sure you fix the stuff at the bottom with the time ranges...
        try:
            await self.connection.execute(
                f"select pg_advisory_lock({DECOMPRESS_BACKFILL_ADVISORY_LOCK});"
            )
            await self.connection.execute(
                f"""
                        DO $BODY$
                        DECLARE
                            compression_job_id int;
                        BEGIN
                            SELECT j.job_id INTO compression_job_id
                                FROM timescaledb_information.jobs j
                                WHERE j.proc_name = 'policy_compression'
                                    AND j.hypertable_name = '{destination_table_name}';

                            PERFORM alter_job(compression_job_id, scheduled => false);
                            PERFORM decompress_chunk(i, if_compressed => true)
                                FROM show_chunks('{destination_table_name}', older_than => '{day_after_day_to_backfill}', newer_than => '{day_to_backfill}') i;
                            INSERT INTO {destination_table_name}
                                SELECT * from {staging_table_name}
                                ON CONFLICT DO NOTHING;
                            PERFORM alter_job(compression_job_id, scheduled => false);
                            CALL run_job(compression_job_id);
                        END
                        $BODY$
                        """
            )
        finally:
            await self.connection.execute(
                f"select pg_advisory_unlock({DECOMPRESS_BACKFILL_ADVISORY_LOCK});"
            )

    async def drop_staging_table(self, staging_table_name):
        await self.connection.execute(
            f"""
            Drop table {staging_table_name}
            """
        )
