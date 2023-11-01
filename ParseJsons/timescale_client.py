class TimeScaleClient:
    def __init__(self, connection):
        self.connection = connection

    async def create_staging_table(self, staging_table_name):
        """This is needed in order to be able to upsert data. Sometimes values come with duplication"""
        await self.connection.execute(
            f"""create table IF NOT EXISTS {staging_table_name} (like measurements excluding indexes excluding constraints)"""
        )

    async def copy_many_to_table(self, data, table_name):
        await self.connection.copy_records_to_table(table_name=table_name, records=data)

    async def load_temporary_table_to_measurements(self):
        await self.connection.execute(
            """
            INSERT INTO {table}(ts, signal_id, value)
            SELECT * FROM _data
            ON CONFLICT (signal_id,ts)
            DO NOTHING
        """.format(
                table="measurements"
            )
        )

    async def decompress_backfill(self, staging_table_name, destination_table_name):
        await self.connection.execute(
            f"""

            DO $BODY$
            DECLARE
                compress_after interval;
                prev_schedule_interval interval;
            BEGIN
                SELECT json_extract_path(config::json, 'compress_after')::text::interval INTO compress_after FROM timescaledb_information.jobs WHERE
                    proc_name = 'policy_compression' AND
                    hypertable_name  = '{destination_table_name}';

                SELECT schedule_interval::text::interval INTO prev_schedule_interval FROM timescaledb_information.jobs WHERE
                    proc_name = 'policy_compression' AND
                    hypertable_name  = '{destination_table_name}';

                PERFORM remove_compression_policy('{destination_table_name}', if_exists => true);

                CALL decompress_backfill(staging_table := '{staging_table_name}', destination_hypertable := '{destination_table_name}', on_conflict_action := 'NOTHING');

                PERFORM add_compression_policy('{destination_table_name}', compress_after => compress_after, schedule_interval => prev_schedule_interval);
            END;
            $BODY$;
            """
        )
        # see: https://dev.azure.com/Axpo-AXP/AXH-Secret-Module-Development/_git/AXH-Asset-API/commit/d9f1142a4e40e6a0eea85866a8c49af723d74dc9?refName=refs/heads/master&path=/iot_api/app/services/measurement_service/measurement_service.py
        # see: https://github.com/timescale/timescaledb-extras/issues/28




    async def drop_staging_table(self, staging_table_name):
        await self.connection.execute(
            f"""
            Drop table {staging_table_name}
            """
        )
