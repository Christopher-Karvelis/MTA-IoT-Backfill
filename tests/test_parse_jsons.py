import json

import numpy as np
import pandas as pd

from ParseJsons import turn_result_tuple_into_dataframe


class TestParseJsons:

    def test_turn_result_tuple_into_dataframe_add_origin_information(self):
        result_df = turn_result_tuple_into_dataframe(
            ("my_origin", json.dumps([{"measurement_value": 0, "ts": "2022-10-10"}])))

        assert result_df["source"][0] == "my_origin"

    def test_turn_result_tuple_into_dataframe_coerces_numeric(self):
        result_df = turn_result_tuple_into_dataframe(
            ("my_origin", json.dumps(
                [{"measurement_value": 0, "ts": "2022-10-10"},
                 {"measurement_value": "wat", "ts": "2022-11-11"},
                 {"measurement_value": True, "ts": "2022-11-11"}])))

        assert result_df["measurement_value"][0] == 0.0
        assert result_df["measurement_value"].isna()[1]
        assert result_df["measurement_value"][2] == 1.0
