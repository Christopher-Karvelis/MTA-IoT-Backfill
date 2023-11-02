from unittest.mock import ANY

import pytest

from Orchestrator import orchestrator_function, produce_grouped_and_filtered_inputs


@pytest.fixture
def nested_list_of_blob_names():
    return [["2021-12-31/_from_2022-01-01/00", "2022-01-01/_from_2022-01-01/00"],
            ["2022-01-01/_from_2022-01-01/23", "2022-01-02/_from_2022-01-01/23",
             "2021-10-10/_from_2022-01-01/00"]]


class TestOrchestrator:
    def test_calls_parse_json(self, mocker):
        mock_context = mocker.MagicMock()
        mock_context.get_input.return_value = {
            "ts_start": "2022-01-01T00:00:00",
            "ts_end": "2022-01-01T01:00:00",
        }

        gen_orchestrator = orchestrator_function(mock_context)
        next(gen_orchestrator)
        gen_orchestrator.send(["Success", "Success"])

        mock_context.call_activity.assert_any_call(
            "ParseJsons",
            input_={
                "ts_end": "2022-01-01T01:00:00",
                "ts_start": "2022-01-01T00:00:00",
            },
        )

    def test_calls_decompress_backfill_for_appropriate_days(self, mocker, nested_list_of_blob_names):
        mock_context = mocker.MagicMock()
        mock_context.get_input.return_value = {
            "ts_start": "2022-01-01T00:00:00",
            "ts_end": "2022-01-02T01:00:00",
        }

        gen_orchestrator = orchestrator_function(mock_context)
        next(gen_orchestrator)
        next(gen_orchestrator)
        gen_orchestrator.send(nested_list_of_blob_names)

        mock_context.call_sub_orchestrator.assert_any_call(
            "BackfillOrchestrator",
            {"day_to_backfill": "2021-12-31", "blob_names": ["2021-12-31/_from_2022-01-01/00"]}
        )

        assert mock_context.call_sub_orchestrator.call_count == 3

    def test_can_group_nested_list_of_blob_names(self, nested_list_of_blob_names):
        grouped = produce_grouped_and_filtered_inputs({
            "ts_start": "2000-01-01T00:00:00",
            "ts_end": "2100-01-01T01:00:00",
        }, nested_list_of_blob_names)

        assert len(grouped) == 4
        assert {"blob_names": ["2022-01-02/_from_2022-01-01/23"],
                "day_to_backfill": "2022-01-02"} in grouped
        assert {"blob_names": ["2021-12-31/_from_2022-01-01/00"],
                "day_to_backfill": "2021-12-31"} in grouped
        assert {"blob_names": ["2021-10-10/_from_2022-01-01/00"],
                "day_to_backfill": "2021-10-10"} in grouped
        assert {"blob_names": ["2022-01-01/_from_2022-01-01/00",
                               "2022-01-01/_from_2022-01-01/23"],
                "day_to_backfill": "2022-01-01"} in grouped

    def test_can_filter_based_on_ts_start_and_end(self, nested_list_of_blob_names):
        grouped = produce_grouped_and_filtered_inputs({
            "ts_start": "2022-01-01T00:00:00",
            "ts_end": "2022-01-02T00:00:00",
        }, nested_list_of_blob_names)

        assert len(grouped) == 3
        assert {"blob_names": ["2022-01-02/_from_2022-01-01/23"],
                "day_to_backfill": "2022-01-02"} in grouped
        assert {"blob_names": ["2021-12-31/_from_2022-01-01/00"],
                "day_to_backfill": "2021-12-31"} in grouped
        assert {"blob_names": ["2022-01-01/_from_2022-01-01/00",
                               "2022-01-01/_from_2022-01-01/23"],
                "day_to_backfill": "2022-01-01"} in grouped
