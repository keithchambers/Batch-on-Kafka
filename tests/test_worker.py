import unittest
from unittest import mock

from batch import worker


class WorkerTests(unittest.TestCase):
    def test_validate_rows_accepts_supported_types(self):
        rows = [
            {
                "event_id": "1",
                "timestamp": "1716045542",
                "amount": "12.5",
            }
        ]
        schema = {
            "event_id": "String",
            "timestamp": "UInt32",
            "amount": "Float64",
        }

        valid_rows, rejected_rows, invalid_row_count = worker._validate_rows(rows, schema)

        self.assertEqual(
            valid_rows,
            [{"event_id": "1", "timestamp": 1716045542, "amount": 12.5}],
        )
        self.assertEqual(rejected_rows, [])
        self.assertEqual(invalid_row_count, 0)

    def test_validate_rows_rejects_invalid_values(self):
        rows = [
            {
                "event_id": "1",
                "timestamp": "bad",
                "amount": "",
            }
        ]
        schema = {
            "event_id": "String",
            "timestamp": "UInt32",
            "amount": "Float64",
        }

        valid_rows, rejected_rows, invalid_row_count = worker._validate_rows(rows, schema)

        self.assertEqual(valid_rows, [])
        self.assertEqual(invalid_row_count, 1)
        self.assertEqual({row["column"] for row in rejected_rows}, {"timestamp", "amount"})
        self.assertEqual(
            {row["error"] for row in rejected_rows},
            {"INVALID_UINT32", "MISSING_COLUMN"},
        )

    @mock.patch("batch.worker.requests.get")
    def test_job_is_cancelled_returns_true_for_cancelled_job(self, mock_get):
        mock_get.return_value.json.return_value = {"state": "CANCELLED"}
        mock_get.return_value.raise_for_status.return_value = None

        self.assertTrue(worker._job_is_cancelled("job123"))

    @mock.patch("batch.worker.requests.get")
    def test_job_is_cancelled_returns_false_for_active_job(self, mock_get):
        mock_get.return_value.json.return_value = {"state": "SUCCESS"}
        mock_get.return_value.raise_for_status.return_value = None

        self.assertFalse(worker._job_is_cancelled("job123"))

    @mock.patch("batch.worker.logger.exception")
    @mock.patch("batch.worker.requests.get", side_effect=RuntimeError("boom"))
    def test_job_is_cancelled_or_false_returns_false_when_lookup_fails(
        self,
        mock_get,
        mock_log_exception,
    ):
        self.assertFalse(worker._job_is_cancelled_or_false("job123"))
        mock_get.assert_called_once()
        mock_log_exception.assert_called_once()

    def test_processing_delay_applies_cancellation_grace_window(self):
        self.assertEqual(worker._processing_delay_ms(1_000, 1_100), 400)
        self.assertEqual(worker._processing_delay_ms(1_000, 1_500), 0)
