from cmath import log
import logging
import json
import azure.functions as func
import pandas as pd
from pgcopy import CopyManager

from .timescale_client import TimescaleClient



class AzureFunctionStreaming:
    # Load data --> later directly from mssql
    def __init__(self) -> None:
        sensors = pd.read_csv("Sensor.csv")
        reduced_sensor_data = sensors[['SensorId', 'SensorName', 'Plant']]
        self.sensor_table = reduced_sensor_data
        reduced_sensor_data["Unique"] = reduced_sensor_data['SensorName'] + reduced_sensor_data['Plant']
        reduced_sensor_data["Hash"] = reduced_sensor_data['Unique'].apply(hash)
        self.hash_table = pd.DataFrame(index=reduced_sensor_data["Hash"], columns=['SensorId'], data=reduced_sensor_data['SensorId'].values)

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
                        #entry['Value']['SourceTimestamp'],
                        self.hash_table.loc[hash(entry["NodeId"].partition(';s=')[2]+entry["DisplayName"].partition("_")[0])].values[0],
                        entry['Value']['Value'] 
                    )
                )
            except:
                # TODO --> send message to HTD. 
                pass 

        # upload
        self.mgr.copy(values)
        self.conn.commit()
        logging.info(f"Uploading blob {myblob.name} was successful")



