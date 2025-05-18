import unittest
import tempfile
import os
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import HTTPException

from batch.api import _peek_validate


class ValidationTests(unittest.TestCase):
    def test_valid_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "data.csv")
            with open(p, "w") as f:
                f.write("col\n1")
            _peek_validate(p, ".csv")

    def test_invalid_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "bad.csv")
            with open(p, "wb") as f:
                f.write(b"\xff\xff")
            with self.assertRaises(HTTPException):
                _peek_validate(p, ".csv")

    def test_valid_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "data.parquet")
            table = pa.table({"a": [1, 2]})
            pq.write_table(table, p)
            _peek_validate(p, ".parquet")

    def test_invalid_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "bad.parquet")
            with open(p, "w") as f:
                f.write("not parquet")
            with self.assertRaises(HTTPException):
                _peek_validate(p, ".parquet")

    def test_unsupported(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "foo.txt")
            with open(p, "w") as f:
                f.write("data")
            with self.assertRaises(HTTPException):
                _peek_validate(p, ".txt")


if __name__ == "__main__":
    unittest.main()
