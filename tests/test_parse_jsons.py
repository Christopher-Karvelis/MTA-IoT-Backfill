import json

import numpy as np
import pandas as pd

from ParseJsons import turn_result_tuple_into_dataframe, prepare_dataframe


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

    def test_prepare_dataframe_adds_signal_information(self):
        my_df = pd.DataFrame({"measurement_value": [1, 2],
                              "control_system_identifier": ["ide1", "ide2"],
                              "plant": ["plant", "plant"],
                              "ts": pd.to_datetime(["2022-10-10", "2022-10-11"])
                              })
        signal_hash_table = {"ide1plant": 1, "ide2plant": 3}
        result = prepare_dataframe(my_df, signal_hash_table)
        np.testing.assert_array_equal(result["signal_id"].values, [1, 3])

    def test_prepare_dataframe_can_deal_with_non_existing_hash_hit(self):
        my_df = pd.DataFrame({"measurement_value": [1, 2, 3],
                              "control_system_identifier": ["ide1", "ide2", "ide3"],
                              "plant": ["plant", "plant", "unknown_plant"],
                              "ts": pd.to_datetime(["2022-10-10", "2022-10-11", "2022-10-12"])
                              })
        signal_hash_table = {"ide1plant": 1, "ide2plant": 3}
        result = prepare_dataframe(my_df, signal_hash_table)
        np.testing.assert_array_equal(result["signal_id"].values, [1, 3, np.nan])
