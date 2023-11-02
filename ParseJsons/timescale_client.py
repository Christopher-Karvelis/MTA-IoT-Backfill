import datetime

DECOMPRESS_BACKFILL_ADVISORY_LOCK = 345678


def _produce_day_after_day_to_backfill(day_to_backfill):
    return (datetime.date.fromisoformat(day_to_backfill) + datetime.timedelta(days=1)).isoformat()


class TimeScaleClient:
    def __init__(self, connection):
        self.connection = connection

    async def create_staging_table(self, staging_table_name):
        """This is needed in order to be able to upsert data. Sometimes values come with duplication"""
        await self.connection.execute(
            f"""create table IF NOT EXISTS {staging_table_name} (like measurements excluding indexes excluding constraints)"""
        )
        # we could add a constraint here on the datetimes...
        # CREATE TABLE your_table (

    #  id serial PRIMARY KEY,
    #  event_time timestamp,
    #  CONSTRAINT event_time_range_check CHECK (event_time >= '2023-01-01 00:00:00' AND event_time <= '2023-12-31 23:59:59')
    # );

    async def copy_many_to_table(self, data, table_name):
        await self.connection.copy_records_to_table(table_name=table_name, records=data)

    async def decompress_backfill(self, day_to_backfill, staging_table_name, destination_table_name):
        day_after_day_to_backfill = _produce_day_after_day_to_backfill(day_to_backfill)
        # make sure you fix the stuff at the bottom with the time ranges...
        try:
            await self.connection.execute(f"select pg_advisory_lock({DECOMPRESS_BACKFILL_ADVISORY_LOCK});")
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
            await self.connection.execute(f"select pg_advisory_unlock({DECOMPRESS_BACKFILL_ADVISORY_LOCK});")

            # https://docs.timescale.com/mst/latest/troubleshooting/

    async def drop_staging_table(self, staging_table_name):
        await self.connection.execute(
            f"""
            Drop table {staging_table_name}
            """
        )
