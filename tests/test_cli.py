import re
from batch import cli


def test_progress_bar_complete():
    bar = cli._progress_bar(100, 100, "SUCCESS")
    assert bar == "[#################]"


def test_format_jobs():
    job = {
        "id": "a1b2c3d4",
        "model_id": "fraud_detector",
        "state": "SUCCESS",
        "totals": {"rows": 1000, "ok": 1000, "errors": 0},
        "timings": {"waiting_ms": 1000, "processing_ms": 2000},
    }
    out = cli._format_jobs([job])
    # should contain progress bar and percent
    assert "[#################]" in out
    assert re.search(r"100%", out)
    # waiting/processing should show 00:01 and 00:02
    assert "00:01" in out
    assert "00:02" in out
