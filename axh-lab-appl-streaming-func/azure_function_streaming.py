from cmath import log
import logging
import json
import azure.functions as func
import pandas as pd
from pgcopy import CopyManager
import pymsteams
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from datetime import date
from .timescale_client import TimescaleClient

load_dotenv()


class AzureFunctionStreaming:
    def __init__(self) -> None:
        # Load data
        sensors = pd.read_csv("Sensor.csv")
        reduced_sensor_data = sensors[['SensorId', 'SensorName', 'Plant']]
        self.sensor_table = reduced_sensor_data
        reduced_sensor_data["Unique"] = reduced_sensor_data['SensorName'] + reduced_sensor_data['Plant']
        reduced_sensor_data["Hash"] = reduced_sensor_data['Unique'].apply(hash)
        self.hash_table = pd.DataFrame(index=reduced_sensor_data["Hash"], columns=['SensorId'], data=reduced_sensor_data['SensorId'].values)

        # undefined Sensors
        self.undefined_sensors = pd.DataFrame(columns=[['SensorName', 'Plant']])
        self.myTeamsMessage = pymsteams.connectorcard(os.getenv("TEAMS_URL"))
        self.myTeamsMessage.addLinkButton("Check this container for missing sensors", os.getenv("LINK_TO_STORAGE_ACCOUNT"))
        connection_string = os.getenv("AzureWebJobsStorage")
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.blob_client = self.blob_service_client.get_blob_client(container=os.getenv("AZURE_CONTAINER_NAME"), blob=f"missing-sensors-{date.today().strftime('%Y-%m-%d')}.csv")

        # Timescale
        timescale_client = TimescaleClient()
        self.conn = timescale_client.get_connection()
        cols = ['ts', 'sensorid', 'measurementvalue']
        self.cursor = self.conn.cursor()
        self.mgr = CopyManager(self.conn, 'measurements', cols)


    async def input(self, myblob: func.InputStream):
        logging.info(f"Name: {myblob.name}  "
                    f"Blob Size: {myblob.length} bytes  ")
        
        jsonData=json.load(myblob)
        values = []
        send_message = False
        for entry in jsonData:
            try:
                values.append(
                    (
                        pd.to_datetime(entry['Value']['SourceTimestamp']).to_pydatetime(), 
                        self.hash_table.loc[hash(entry["NodeId"].partition(';s=')[2]+entry["DisplayName"].partition("_")[0])].values[0],
                        entry['Value']['Value'] 
                    )
                )
            except:
                try:
                    self.undefined_sensors.loc[hash(entry["NodeId"].partition(';s=')[2]+entry["DisplayName"].partition("_")[0])]
                except:
                    logging.info(f"Missing Sensor coming from OPC: {entry['NodeId'].partition(';s=')[2]}")
                    logging.info(f"Number of undefined Sensors coming from OPC: {self.undefined_sensors.shape[0]}")
                    self.undefined_sensors.loc[hash(entry["NodeId"].partition(';s=')[2]+entry["DisplayName"].partition("_")[0])] = [
                        entry["NodeId"].partition(';s=')[2], 
                        entry["DisplayName"].partition("_")[0]
                    ]
                    send_message = True

        if send_message:
            self.blob_client.upload_blob(data=self.undefined_sensors.to_csv(), overwrite=True)
            self.myTeamsMessage.text(f"New missing sensor is added to missing-sensors-{date.today().strftime('%Y-%m-%d')}.csv")
            self.myTeamsMessage.send()
            
        # upload
        try: 
            self.mgr.copy(values)
            self.conn.commit()
            logging.info(f"Uploading blob {myblob.name} was successful")

        except:
            try:
                self.conn.rollback()
                self.mgr.copy(values)
                self.conn.commit()
            except Exception as err:
                logging.error(f"Exception occured during upload {err.message}")



