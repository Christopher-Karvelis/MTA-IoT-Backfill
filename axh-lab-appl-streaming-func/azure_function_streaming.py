import logging
import json
import azure.functions as func
from pgcopy import CopyManager
from psycopg2 import OperationalError
import pymsteams
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from datetime import date
from .timescale_client import TimescaleClient
from .signal_client import SignalClient
from keycloak.keycloak_openid import KeycloakOpenID  # type: ignore

load_dotenv()

logger = logging.getLogger('azure')
logger.setLevel(logging.ERROR)
logger.setLevel(logging.INFO)


class AzureFunctionStreaming:
    def __init__(self) -> None:
        # Load data
        signal_client = SignalClient()
        self.hash_table = signal_client.provide_hash_table()
        logger.info("Signal Table Loaded")

        # undefined Sensors
        self.undefined_sensors = pd.DataFrame(columns=[['control_system_identifier', 'plant']])
        self.myTeamsMessage = pymsteams.connectorcard(os.getenv("TEAMS_URL"))
        self.myTeamsMessage.addLinkButton("Check this container for missing sensors", os.getenv("LINK_TO_STORAGE_ACCOUNT"))
        connection_string = os.getenv("AzureWebJobsStorage")
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string, logging_enable=False)
        self.blob_client = self.blob_service_client.get_blob_client(container=os.getenv("AZURE_CONTAINER_NAME"), blob=f"missing-sensors-{date.today().strftime('%Y-%m-%d')}.csv")

        # Timescale
        timescale_client = TimescaleClient()
        self.conn = timescale_client.get_connection()
        cols = ['ts', 'sensorid', 'measurementvalue']
        self.mgr = CopyManager(self.conn, 'measurements', cols)

    async def input(self, myblob: func.InputStream):
        logger.info(f"Name: {myblob.name}  "
                    f"Blob Size: {myblob.length} bytes  ")
        
        json_data = json.load(myblob)
        values = []
        send_message = False
        for entry in json_data:
            name = entry["NodeId"].partition('#s=')[2].replace('%3a', ':').replace('%2f', '/')
            plant = entry["DisplayName"].partition("_")[0]
            try:
                values.append(
                    (
                        pd.to_datetime(entry['Value']['SourceTimestamp']).to_pydatetime(), 
                        self.hash_table.loc[hash(name + plant)].values[0],
                        entry['Value']['Value'] 
                    )
                )
            except KeyError:
                try:
                    self.undefined_sensors.loc[hash(name + plant)]
                except KeyError:
                        logger.info(f"Missing Sensor coming from OPC: {name}")
                        logger.info(f"Number of undefined Sensors coming from OPC: {self.undefined_sensors.shape[0]}")
                        self.undefined_sensors.loc[hash(name + plant)] = [
                            name,
                            plant
                        ]
                        send_message = True

        if send_message:
            self.blob_client.upload_blob(data=self.undefined_sensors.to_csv(), overwrite=True)
            self.myTeamsMessage.text(f"New missing sensor is added to missing-sensors-{date.today().strftime('%Y-%m-%d')}.csv")
            self.myTeamsMessage.send()
        try:
            self.conn.isolation_level
        except OperationalError as oe:
            timescale_client = TimescaleClient()
            self.conn = timescale_client.get_connection()
            logger.info(f"New connection needs to be established: {oe}")
        self.mgr.copy(values)
        self.conn.commit()

        logger.info(f"Uploading blob {myblob.name} was successful")



