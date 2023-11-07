import json

from ParseJsons import turn_result_tuple_into_dataframe


class TestParseJsons:

    def test_turn_result_tuple_into_dataframe_add_origin_information(self):
        result_df = turn_result_tuple_into_dataframe(
            ("my_origin", json.dumps([{"value": 0, "ts": "2022-10-10"}])))

        assert result_df["source"][0] == "my_origin"

    def test_turn_result_tuple_into_dataframe_coerces_numeric(self):
        result_df = turn_result_tuple_into_dataframe(
            ("my_origin", json.dumps(
                [{"value": 0, "ts": "2022-10-10"},
                 {"value": "wat", "ts": "2022-11-11"},
                 {"value": True, "ts": "2022-11-11"}])))

        assert result_df["value"][0] == 0.0
        assert result_df["value"].isna()[1]
        assert result_df["value"][2] == 1.0
