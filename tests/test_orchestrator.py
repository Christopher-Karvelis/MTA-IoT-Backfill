import pytest
from unittest.mock import ANY
from shared_assets import helpers
from parse_jsons.json_to_parquet import json_to_parquet_orchestrator1
from backfill.decompress_backfill import backfill_orchestrator_concrete
from shared_assets.input_structure import InputParameters, BackFillInputParameters


@pytest.fixture
def list_of_blob_names():
    return [
        "2021-12-31/_from_2022-01-01/00", 
        "2022-01-01/_from_2022-01-01/00",
        "2022-01-01/_from_2022-01-01/23",
        "2022-01-02/_from_2022-01-01/23",
        "2021-10-10/_from_2022-01-01/00",
    ]


class TestOrchestrator:
    def test_calls_decompress_backfill_for_appropriate_days(
        self, mocker, list_of_blob_names
    ):
        mock_context = mocker.MagicMock()
        mock_context.get_input.return_value = BackFillInputParameters(**{
            "ts_start": "2022-01-01T00:00:00",
            "ts_end": "2022-01-02T01:00:00",
            "blobs_to_consider": list_of_blob_names
        })
       
        gen_orchestrator = backfill_orchestrator_concrete(mock_context)
        try:
            next(gen_orchestrator)
            next(gen_orchestrator)
            gen_orchestrator.send(list_of_blob_names)
        except StopIteration as e:
                result = e.value

        mock_context.call_sub_orchestrator.assert_any_call(
            "backfill_sub_orchestrator",
            {
                "day_to_backfill": "2021-12-31",
                "blob_names": ["2021-12-31/_from_2022-01-01/00"],
            },
        )

        assert mock_context.call_sub_orchestrator.call_count == 3

    def test_can_group_nested_list_of_blob_names(self, list_of_blob_names):
        grouped = helpers.produce_grouped_and_filtered_inputs(
            BackFillInputParameters(**{
                "ts_start": "2000-01-01T00:00:00",
                "ts_end": "2100-01-01T01:00:00",
                "blobs_to_consider": list_of_blob_names
            })
        )

        assert len(grouped) == 4
        assert {
            "blob_names": ["2022-01-02/_from_2022-01-01/23"],
            "day_to_backfill": "2022-01-02",
        } in grouped
        assert {
            "blob_names": ["2021-12-31/_from_2022-01-01/00"],
            "day_to_backfill": "2021-12-31",
        } in grouped
        assert {
            "blob_names": ["2021-10-10/_from_2022-01-01/00"],
            "day_to_backfill": "2021-10-10",
        } in grouped
        assert {
            "blob_names": [
                "2022-01-01/_from_2022-01-01/00",
                "2022-01-01/_from_2022-01-01/23",
            ],
            "day_to_backfill": "2022-01-01",
        } in grouped

    def test_can_filter_based_on_ts_start_and_end(self, list_of_blob_names):
        grouped = helpers.produce_grouped_and_filtered_inputs(
           BackFillInputParameters(**{
                "ts_start": "2022-01-01T00:00:00",
                "ts_end": "2022-01-02T00:00:00",
                "blobs_to_consider": list_of_blob_names
            })
        )

        assert len(grouped) == 3
        assert {
            "blob_names": ["2022-01-02/_from_2022-01-01/23"],
            "day_to_backfill": "2022-01-02",
        } in grouped
        assert {
            "blob_names": ["2021-12-31/_from_2022-01-01/00"],
            "day_to_backfill": "2021-12-31",
        } in grouped
        assert {
            "blob_names": [
                "2022-01-01/_from_2022-01-01/00",
                "2022-01-01/_from_2022-01-01/23",
            ],
            "day_to_backfill": "2022-01-01",
        } in grouped
