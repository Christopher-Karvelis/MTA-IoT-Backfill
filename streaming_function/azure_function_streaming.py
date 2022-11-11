import json
import logging
import os
from datetime import datetime, timedelta, timezone

import asyncpg
import azure.functions as func
import pandas as pd
from dotenv import load_dotenv

from streaming_function.blob_storge_writer import BlobStorageWriter
from streaming_function.signal_client import SignalClient
from streaming_function.timescale_client import TimeScaleClient

load_dotenv()

logger = logging.getLogger("azure")
logger.setLevel(logging.ERROR)
logger.setLevel(logging.INFO)


class AzureFunctionStreaming:
    def __init__(self) -> None:
        # Load data
        signal_client = SignalClient()
        self.hash_table = signal_client.provide_hash_table()
        self.hash_table.to_csv("Signal_Hash_Table.csv")
        logger.info("Signal Table Loaded")

        self.rejected_signals_blob_writer = BlobStorageWriter(logger=logger)

        # Timescale
        self.password = os.getenv("TIMESCALE_PASSWORD")
        self.username = os.getenv("TIMESCALE_USERNAME")
        self.host = os.getenv("TIMESCALE_HOST_URL")
        self.port = os.getenv("TIMESCALE_PORT")
        self.dbname = os.getenv("TIMESCALE_DATABASE_NAME")

    async def input(self, json_blob: func.InputStream):

        conn = await asyncpg.connect(
            f"postgres://{self.username}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        )

        timescale_client = TimeScaleClient(connection=conn)

        logger.info(
            f"Name: {json_blob.name}  " f"Blob Size: {json_blob.length} bytes  "
        )

        json_data = json.load(json_blob)
        values = []
        rejected_by_streaming = []
        count = 0
        for entry in json_data:
            count += 1
            control_system_identifier = entry["control_system_identifier"]
            plant = entry["plant"]
            try:
                values.append(
                    (
                        pd.to_datetime(entry["ts"]).to_pydatetime(),
                        self.hash_table.loc[
                            hash(control_system_identifier + plant)
                        ].values[0],
                        entry["measurement_value"],
                    )
                )
            except KeyError:
                rejected_by_streaming.append(entry)

        if len(rejected_by_streaming) > 0:
            await self.rejected_signals_blob_writer.upload_blob(rejected_by_streaming)

        logger.info(f"Total numbers processed in Blob {count}")
        unique = list(set(values))

        remove_nan = [i for i in unique if type(i[2]) != str]

        time = datetime.now(timezone.utc)
        data_younger_than_8_hours = [
            i for i in remove_nan if i[0] > time - timedelta(hours=8)
        ]

        await timescale_client.create_temporary_table()
        await timescale_client.copy_records_to_temporary_table(
            data=data_younger_than_8_hours
        )
        await timescale_client.load_temporary_table_to_measurements()
        await conn.close()

        def zero_div(x, y):
            return y and x / y or 0

        logger.info(f"Uploading blob {json_blob.name} was successful")
        logger.info(
            f"Uploaded {len(data_younger_than_8_hours)} to {json_blob.name} --"
            f" fraction of uploaded/processed {zero_div(len(data_younger_than_8_hours), count)}"
        )
