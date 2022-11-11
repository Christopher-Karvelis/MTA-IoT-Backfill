class TimeScaleClient:
    def __init__(self, connection):
        self.connection = connection

    async def create_temporary_table(self):
        await self.connection.execute(
            """CREATE TEMPORARY TABLE _data(
            ts TIMESTAMPTZ, signal_id INTEGER, measurement_value DOUBLE PRECISION
        )"""
        )

    async def copy_records_to_temporary_table(self, data):
        await self.connection.copy_records_to_table("_data", records=data)

    async def load_temporary_table_to_measurements(self):
        await self.connection.execute(
            """
            INSERT INTO {table}(ts, signal_id, measurement_value)
            SELECT * FROM _data
            ON CONFLICT (signal_id,ts)
            DO NOTHING
        """.format(
                table="measurements"
            )
        )
