import json
import logging
import os
from datetime import date, datetime, timedelta, timezone

import asyncpg
import azure.functions as func
import pandas as pd
import pymsteams
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

from .signal_client import SignalClient

load_dotenv()

logger = logging.getLogger("azure")
logger.setLevel(logging.ERROR)
logger.setLevel(logging.INFO)


class AzureFunctionStreaming:
    def __init__(self) -> None:
        # Load data
        if os.getenv("RELOAD_SIGNAL_TABLE"):
            signal_client = SignalClient()
            self.hash_table = signal_client.provide_hash_table()
            self.hash_table.to_csv("Signal_Hash_Table.csv")
            logger.info("Signal Table Loaded")
        else:
            self.hash_table = pd.read_csv("Signal_Hash_Table.csv")

        # undefined Sensors
        self.undefined_sensors = pd.DataFrame(
            columns=[["control_system_identifier", "plant"]]
        )
        self.myTeamsMessage = pymsteams.connectorcard(os.getenv("TEAMS_URL"))
        self.myTeamsMessage.addLinkButton(
            "Check this container for missing sensors",
            os.getenv("LINK_TO_STORAGE_ACCOUNT"),
        )
        connection_string = os.getenv("AzureWebJobsStorage")
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string, logging_enable=False
        )
        self.blob_client = self.blob_service_client.get_blob_client(
            container=os.getenv("AZURE_CONTAINER_NAME"),
            blob=f"missing-sensors-{date.today().strftime('%Y-%m-%d')}.csv",
        )

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
        send_message = False
        for entry in json_data:
            name = (
                entry["NodeId"]
                .partition("s=")[2]
                .replace("%3a", ":")
                .replace("%2f", "/")
            )
            plant = entry["DisplayName"].partition("_")[0]
            try:
                values.append(
                    (
                        pd.to_datetime(
                            entry["Value"]["SourceTimestamp"]
                        ).to_pydatetime(),
                        self.hash_table.loc[hash(name + plant)].values[0],
                        entry["Value"]["Value"],
                    )
                )
            except KeyError:
                try:
                    self.undefined_sensors.loc[hash(name + plant)]
                except KeyError:
                    logger.info(f"Missing Sensor coming from OPC: {name}")
                    logger.info(
                        f"Number of undefined Sensors coming from OPC: {self.undefined_sensors.shape[0]}"
                    )
                    self.undefined_sensors.loc[hash(name + plant)] = [name, plant]
                    send_message = True

        if send_message:
            self.blob_client.upload_blob(
                data=self.undefined_sensors.to_csv(), overwrite=True
            )
            self.myTeamsMessage.text(
                f"New missing sensor is added to missing-sensors-{date.today().strftime('%Y-%m-%d')}.csv"
            )
            self.myTeamsMessage.send()

        unique = list(set(values))

        remove_nan = [i for i in unique if type(i[2]) != str]

        time = datetime.now(timezone.utc)
        drop_old_data = [i for i in remove_nan if i[0] > time - timedelta(hours=6)]

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
            ON CONFLICT (ts, signal_id)
            DO NOTHING
        """.format(
                table="measurements"
            )
        )

        # await conn.copy_records_to_table("measurements", records=drop_old_data)

        await conn.close()

        logger.info(f"Uploading blob {myblob.name} was successful")
        logger.info(f"Uploaded {len(drop_old_data)} to {myblob.name}")
