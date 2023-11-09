import numpy as np
import pandas as pd

from UploadToStagingTable import prepare_dataframe


class TestUploadToStagingTable:

    def test_filters_nan_and_drops_columns(self):
        df = pd.DataFrame(
            {"ts": pd.to_datetime(["2022-10-10", "2022-10-11"]),
             "signal_id": [1, 2],
             "value": [23.1, np.NAN],
             "control_system_identifier": ["some sht", "cmon"]})
        result = prepare_dataframe(df)
        assert len(result.columns) == 3
        assert len(result) == 1

    def test_filters_nan_in_signal_id_and_drops_columns(self):
        df = pd.DataFrame(
            {"ts": pd.to_datetime(["2022-10-10", "2022-10-11", "2022-10-12"]),
             "signal_id": [1, 2, np.NAN],
             "value": [23.1, np.NAN, 23],
             "control_system_identifier": ["some sht", "cmon", "cmoncmon"]})
        result = prepare_dataframe(df)
        assert len(result.columns) == 3
        assert len(result) == 1

    def test_filters_infinities(self):
        df = pd.DataFrame(
            {"ts": pd.to_datetime(["2022-10-10", "2022-10-11", "2022-10-11"]),
             "signal_id": [1, 2, 3],
             "value": [23.1, np.inf, -np.inf],
             "control_system_identifier": ["some sht", "cmon", "wat"]})
        result = prepare_dataframe(df)
        assert len(result) == 1
