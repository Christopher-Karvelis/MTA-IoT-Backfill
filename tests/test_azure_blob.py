import pandas as pd
import pytest
from shared_assets.azure_blob import _produce_parquet_bytes


class TestAzureBlob:

    @pytest.mark.asyncio
    async def test_upload_data_does_not_complain_about_truncating_timespans(self):
        df = pd.DataFrame({"ts": pd.to_datetime(
            ["2020-09-01T01:02:03.123456789",
             "2020-10-11T00:00:01.000000000001",
             "2020-10-11T00:00:03.000000000001",
             "2020-10-12T00:00:01.000000000001"]),
            "value": [1, 2, 3, 4]})

        bytes = await _produce_parquet_bytes(df)
        df_read = pd.read_parquet(bytes)
        assert len(df_read) == 4
