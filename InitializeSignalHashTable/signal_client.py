import os
from typing import List

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()


class SignalClient:
    def __init__(self):
        self._columns_to_keep = ["signal_id", "control_system_identifier", "plant"]
        self._asset_api_base = os.getenv("ASSET_API_BASE")
        self._data = {
            "grant_type": "client_credentials",
            "client_id": os.getenv("API_CLIENT_ID"),
            "client_secret": os.getenv("API_CLIENT_SECRET"),
        }
        self._auth_header = {"Authorization": "Bearer " + self._provide_token()}
        self._power_plants = self._provide_power_plants()
        self._signals = self._provide_signal_table_with_power_plants()

    def _provide_token(self) -> str:
        res = requests.post(
            "https://accounts.withaxpo.com/auth/realms/axh/protocol/openid-connect/token",
            data=self._data,
        )
        return res.json()["access_token"]

    def _provide_signal_data(self, power_plant: str) -> pd.DataFrame:
        res = requests.get(
            f"{self._asset_api_base}/signals?limit={100000}&offset={0}&plant={power_plant}",
            headers=self._auth_header,
        )
        if res.status_code != 200:
            raise Exception(
                f"Data request failed with status code {res.status_code} "
                f"and details: {res.json()}"
            )
        signals = res.json()
        return pd.json_normalize(signals["records"])

    def _provide_power_plants(self) -> List[str]:
        res = requests.get(
            f"{self._asset_api_base}/assets?asset_type_identifier=power_plant",
            headers=self._auth_header,
        )
        if res.status_code != 200:
            raise Exception(
                f"Data request failed with status code {res.status_code} "
                f"and details: {res.json()}"
            )
        return [i["name"] for i in res.json()]

    def _provide_signal_table_with_power_plants(self) -> pd.DataFrame:
        signal_list = []
        for plant in self._power_plants:
            data = self._provide_signal_data(plant)
            data["plant"] = plant
            signal_list.append(data)

        return pd.concat(signal_list, ignore_index=True)

    def provide_hash_table(self) -> dict:
        reduced_sensor_data = self._signals[self._columns_to_keep]
        reduced_sensor_data_1 = reduced_sensor_data.copy()
        reduced_sensor_data_1["Unique"] = (
            reduced_sensor_data["control_system_identifier"]
            + reduced_sensor_data["plant"]
        )
        reduced_sensor_data_2 = reduced_sensor_data_1.copy()
        return dict(
            zip(reduced_sensor_data_2["Unique"], reduced_sensor_data_2["signal_id"])
        )
