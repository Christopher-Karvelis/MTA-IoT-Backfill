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
from io import BytesIO
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
        self.myTeamsMessage = pymsteams.connectorcard("https://axpogrp.webhook.office.com/webhookb2/1c7d8e30-f530-4faf-acc0-7ef098a2a388@8619c67c-945a-48ae-8e77-35b1b71c9b98/IncomingWebhook/5b6d6935cb774847950f3f9cd081c0cb/adfeab72-7d9c-4e19-a21b-6781b139b707")
        self.myTeamsMessage.addLinkButton("Check this container for missing sensors", "https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2F58b564b2-e2b7-4d4b-b168-5554c5a2aa5f%2FresourceGroups%2Faxh-lab-appl-streaming-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Faxh4lab4streaming4sa/path/missing-sensors/defaultId/properties/publicAccessVal/None")
        connection_string = os.getenv("AZURE_STORAGE_CONNECTIONSTRING")
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
                    self.blob_client.upload_blob(data=self.undefined_sensors.to_csv(), overwrite=True)
                    self.myTeamsMessage.text(f"New missing sensor is added to missing-sensors-{date.today().strftime('%Y-%m-%d')}.csv")
                    self.myTeamsMessage.send()
                pass 

        # upload
        self.mgr.copy(values)
        self.conn.commit()
        logging.info(f"Uploading blob {myblob.name} was successful")



