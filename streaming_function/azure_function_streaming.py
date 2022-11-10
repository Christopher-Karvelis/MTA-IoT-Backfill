import json
import logging
import os
from datetime import datetime, timedelta, timezone

import asyncpg
import azure.functions as func
import pandas as pd
from dotenv import load_dotenv

from .signal_client import SignalClient

load_dotenv()

logger = logging.getLogger("azure")
logger.setLevel(logging.ERROR)
logger.setLevel(logging.INFO)


class AzureFunctionStreaming:
    def __init__(self) -> None:
        # Load data
        if os.getenv("RELOAD_SIGNAL_TABLE", "False").lower() in (
            "true",
            "1",
            "t",
            "True",
        ):
            signal_client = SignalClient()
            self.hash_table = signal_client.provide_hash_table()
            self.hash_table.to_csv("Signal_Hash_Table.csv")
            logger.info("Signal Table Loaded")
        else:
            self.hash_table = pd.read_csv("Signal_Hash_Table.csv")
            self.hash_table = self.hash_table.set_index("Hash")

        # undefined Sensors
        self.undefined_sensors = pd.DataFrame(
            columns=[["control_system_identifier", "plant"]]
        )
        # connection_string = os.getenv("AzureWebJobsStorage")
        # self.blob_service_client = BlobServiceClient.from_connection_string(
        #    connection_string, logging_enable=False
        # )
        # self.blob_client = self.blob_service_client.get_blob_client(
        #    container=os.getenv("AZURE_CONTAINER_NAME"),
        #    blob=f"missing-sensors-{date.today().strftime('%Y-%m-%d')}.csv",
        # )

        # Timescale
        self.password = os.getenv("TIMESCALE_PASSWORD")
        self.username = os.getenv("TIMESCALE_USERNAME")
        self.host = os.getenv("TIMESCALE_HOST_URL")
        self.port = os.getenv("TIMESCALE_PORT")
        self.dbname = os.getenv("TIMESCALE_DATABASE_NAME")

    async def input(self, myblob: func.InputStream):

        conn = await asyncpg.connect(
            f"postgres://{self.username}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        )

        logger.info(f"Name: {myblob.name}  " f"Blob Size: {myblob.length} bytes  ")

        json_data = json.load(myblob)
        values = []
        # send_message = False
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
                try:
                    self.undefined_sensors.loc[hash(control_system_identifier + plant)]
                except KeyError:
                    logger.info(
                        f"Missing Sensor coming from OPC: {control_system_identifier}"
                    )
                    logger.info(
                        f"Number of undefined Sensors coming from OPC: {self.undefined_sensors.shape[0]}"
                    )
                    self.undefined_sensors.loc[
                        hash(control_system_identifier + plant)
                    ] = [control_system_identifier, plant]
                    # send_message = True

        # if send_message:
        #    self.blob_client.upload_blob(
        #        data=self.undefined_sensors.to_csv(), overwrite=True
        #    )

        logger.info(f"Total numbers processed in Blob {count}")
        unique = list(set(values))

        remove_nan = [i for i in unique if type(i[2]) != str]

        time = datetime.now(timezone.utc)
        drop_old_data = [i for i in remove_nan if i[0] > time - timedelta(hours=8)]

        await conn.execute(
            """CREATE TEMPORARY TABLE _data(
            ts TIMESTAMPTZ, signal_id INTEGER, measurement_value DOUBLE PRECISION
        )"""
        )
        await conn.copy_records_to_table("_data", records=drop_old_data)
        await conn.execute(
            """
            INSERT INTO {table}(ts, signal_id, measurement_value)
            SELECT * FROM _data
            ON CONFLICT (signal_id,ts)
            DO NOTHING
        """.format(
                table="measurements"
            )
        )
        # await conn.copy_records_to_table("measurements", records=drop_old_data)

        await conn.close()

        logger.info(f"Uploading blob {myblob.name} was successful")
        logger.info(
            f"Uploaded {len(drop_old_data)} to {myblob.name} --"
            f" fraction of uploaded/processed {len(drop_old_data)/(count+0.00001)}"
        )
