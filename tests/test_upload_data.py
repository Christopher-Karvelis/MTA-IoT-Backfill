import pandas as pd
import pytest

from ParseJsons import upload_data


class TestGrouping:

    @pytest.mark.asyncio
    async def test_produces_right_groups(self, mocker):
        df = pd.DataFrame({"ts": pd.to_datetime(
            ["2020-10-10T00:00:01", "2020-10-11T00:00:01", "2020-10-11T00:00:03", "2020-10-12T00:00:01"]),
                           "value": [1, 2, 3, 4]})

        mock_container = mocker.AsyncMock()
        await upload_data.upload_grouped_as_parquet(mock_container, df, "read_from")

        assert mock_container.upload_blob.call_count == 3

    @pytest.mark.asyncio
    async def test_does_not_produce_groups_with_gaps(self, mocker):
        df = pd.DataFrame({"ts": pd.to_datetime(
            ["2020-09-01T00:00:01", "2020-10-11T00:00:01", "2020-10-11T00:00:03", "2020-10-12T00:00:01"]),
                           "value": [1, 2, 3, 4]})

        mock_container = mocker.AsyncMock()
        await upload_data.upload_grouped_as_parquet(mock_container, df, "read_from")

        assert mock_container.upload_blob.call_count == 3

