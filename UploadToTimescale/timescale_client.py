class TimeScaleClient:
    def __init__(self, connection):
        self.connection = connection

    async def create_temporary_table(self):
        """This is needed in order to be able to upsert data. Sometimes values come with duplication"""
        await self.connection.execute(
            """CREATE TEMPORARY TABLE _data(
            ts TIMESTAMPTZ, signal_id INTEGER, value DOUBLE PRECISION
        )"""
        )

    async def copy_records_to_temporary_table(self, data):
        await self.connection.copy_records_to_table("_data", records=data)

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
