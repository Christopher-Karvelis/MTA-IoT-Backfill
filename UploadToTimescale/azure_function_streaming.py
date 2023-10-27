import json
import logging
import os
from datetime import datetime

import asyncpg
import azure.functions as func
import pandas as pd
from dotenv import load_dotenv

from UploadToTimescale.signal_client import SignalClient
from UploadToTimescale.timescale_client import TimeScaleClient

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


        # Timescale
        self.password = os.getenv("TIMESCALE_PASSWORD")
        self.username = os.getenv("TIMESCALE_USERNAME")
        self.host = os.getenv("TIMESCALE_HOST_URL")
        self.port = os.getenv("TIMESCALE_PORT")
        self.dbname = os.getenv("TIMESCALE_DATABASE_NAME")

    async def input(self, jsonblob: func.InputStream, date_accepted=datetime(2023, 10, 22)):

        conn = await asyncpg.connect(
            f"postgres://{self.username}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        )

        timescale_client = TimeScaleClient(connection=conn)

        json_data = json.load(jsonblob)
        values = []
        rejected_by_streaming = []
        plants_processed = {}
        not_numbers_per_power_plant = {}
        time_too_old_per_power_plant = {}
        for entry in json_data:
            control_system_identifier = entry["control_system_identifier"]
            plant = entry["plant"]
            if plant not in plants_processed:
                plants_processed[plant] = 1
            else:
                plants_processed[plant] += 1
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
                if type(entry["measurement_value"]) == str:
                    not_numbers_per_power_plant[
                        plant
                    ] = not_numbers_per_power_plant.get(plant, []) + [
                        entry["measurement_value"]
                    ]

                if pd.to_datetime(entry["ts"]).to_pydatetime() <= date_accepted:
                    time_too_old_per_power_plant[plant] = (
                        time_too_old_per_power_plant.get(plant, 0) + 1
                    )

            except KeyError:
                rejected_by_streaming.append(entry)

        unique = list(set(values))
        remove_nan = [i for i in unique if type(i[2]) != str]
        data_within_acceptable_timespan = [
            i
            for i in remove_nan
            if i[0] > date_accepted
        ]

        await timescale_client.create_temporary_table()
        await timescale_client.copy_records_to_temporary_table(
            data=data_within_acceptable_timespan
        )
        await timescale_client.load_temporary_table_to_measurements()
        await conn.close()

        def zero_div(x, y):
            return y and x / y or 0

        plants_processed_reformatted = [{key: value} for key, value in plants_processed.items()]
        not_numbers_per_power_plant_reformatted = [{key: value} for key, value in not_numbers_per_power_plant.items()]
        time_too_old_per_power_plant_reformatted = [{key: value} for key, value in time_too_old_per_power_plant.items()]


        logger.info(
            f"Name: {jsonblob.name}  "
            f"Blob Size: {jsonblob.length} bytes \n"
            f"Total numbers processed in Blob {len(json_data)} \n"
            f"Number of values processed per plant {plants_processed_reformatted} \n"
            f"Number of values rejected i.e. not in our signal list {len(rejected_by_streaming)} \n "
            f"Data in blob is not a number {not_numbers_per_power_plant_reformatted} \n "
            f"Data in blob rejected because older than {date_accepted} "
            f"hours {time_too_old_per_power_plant_reformatted} \n "
            f"Uploading blob {jsonblob.name} was successful \n"
            f"Uploaded {len(data_within_acceptable_timespan)} to {jsonblob.name} -- "
            f"fraction of uploaded/processed {zero_div(len(data_within_acceptable_timespan), len(json_data))}"
        )
