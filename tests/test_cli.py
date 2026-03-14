import unittest
from unittest import mock

from batch import cli


class CliTests(unittest.TestCase):
    @mock.patch("batch.cli.typer.echo")
    def test_echo_json_pretty_prints_output(self, mock_echo):
        cli._echo_json({"id": "a1b2c3d4"})

        mock_echo.assert_called_once_with('{\n  "id": "a1b2c3d4"\n}')

    def test_progress_bar_complete(self):
        bar = cli._progress_bar(100, 100, "SUCCESS")
        self.assertEqual(bar, "[#################]")

    def test_format_jobs(self):
        job = {
            "id": "a1b2c3d4",
            "model_id": "fraud_detector",
            "state": "SUCCESS",
            "totals": {"rows": 1000, "ok": 1000, "errors": 0},
            "timings": {"waiting_ms": 1000, "processing_ms": 2000},
        }
        out = cli._format_jobs([job])
        self.assertIn("[#################]", out)
        self.assertRegex(out, r"100%")
        self.assertIn("00:01", out)
        self.assertIn("00:02", out)
        self.assertIn("PROCESSING", out)

    def test_error_text_prefers_json_detail(self):
        response = mock.Mock()
        response.json.return_value = {"detail": "Model not found"}
        response.text = '{"detail":"Model not found"}'
        response.status_code = 404

        self.assertEqual(cli._error_text(response), "Model not found")

    def test_error_text_falls_back_to_status_code(self):
        response = mock.Mock()
        response.json.side_effect = ValueError("not json")
        response.text = ""
        response.status_code = 503

        self.assertEqual(cli._error_text(response), "request failed with status 503")
