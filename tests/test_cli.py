import unittest
from batch import cli


class CliTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
