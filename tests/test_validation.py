import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi import HTTPException

from batch.api import _peek_validate


def test_valid_csv(tmp_path):
    p = tmp_path / "data.csv"
    p.write_text("col\n1")
    _peek_validate(str(p), ".csv")


def test_invalid_csv(tmp_path):
    p = tmp_path / "bad.csv"
    p.write_bytes(b"\xff\xff")
    with pytest.raises(HTTPException):
        _peek_validate(str(p), ".csv")


def test_valid_parquet(tmp_path):
    p = tmp_path / "data.parquet"
    table = pa.table({"a": [1, 2]})
    pq.write_table(table, p)
    _peek_validate(str(p), ".parquet")


def test_invalid_parquet(tmp_path):
    p = tmp_path / "bad.parquet"
    p.write_text("not parquet")
    with pytest.raises(HTTPException):
        _peek_validate(str(p), ".parquet")


def test_unsupported(tmp_path):
    p = tmp_path / "foo.txt"
    p.write_text("data")
    with pytest.raises(HTTPException):
        _peek_validate(str(p), ".txt")
