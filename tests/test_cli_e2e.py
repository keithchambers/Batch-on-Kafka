import ast
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
API_URL = os.environ.get("BATCH_API_URL", "http://localhost:8000")
CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "batchlocal")


class CliE2ETests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get("BATCH_E2E") != "1":
            raise unittest.SkipTest("set BATCH_E2E=1 to run live CLI end-to-end tests")
        cls.cli = Path(sys.executable).with_name("batch")
        cls.env = os.environ.copy()
        cls.env["BATCH_API_URL"] = API_URL
        cls._wait_for_stack()

    @classmethod
    def _wait_for_stack(cls) -> None:
        deadline = time.time() + 60
        last_error = "stack did not become ready"
        while time.time() < deadline:
            try:
                api_response = requests.get(f"{API_URL}/models", timeout=2)
                api_response.raise_for_status()
                clickhouse_response = requests.get(
                    CLICKHOUSE_URL,
                    params={
                        "user": CLICKHOUSE_USER,
                        "password": CLICKHOUSE_PASSWORD,
                        "query": "SELECT 1",
                    },
                    timeout=2,
                )
                clickhouse_response.raise_for_status()
                return
            except Exception as exc:
                last_error = str(exc)
                time.sleep(1)
        raise RuntimeError(last_error)

    def _run_cli(self, *args: str, timeout: int = 30) -> str:
        proc = subprocess.run(
            [str(self.cli), *args],
            cwd=REPO_ROOT,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if proc.returncode != 0:
            raise AssertionError(
                f"CLI command failed: {' '.join(args)}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
            )
        return proc.stdout.strip()

    def _literal_output(self, *args: str):
        return ast.literal_eval(self._run_cli(*args))

    def _write_file(self, directory: str, name: str, content: str) -> str:
        path = Path(directory, name)
        path.write_text(content, encoding="utf-8")
        return str(path)

    def _wait_for_job_state(self, job_id: str, expected_state: str) -> str:
        deadline = time.time() + 60
        last_output = ""
        while time.time() < deadline:
            last_output = self._run_cli("job", "status", job_id)
            if expected_state in last_output:
                return last_output
            time.sleep(1)
        raise AssertionError(
            f"job {job_id} did not reach {expected_state}\nlast output:\n{last_output}"
        )

    def _query_clickhouse(self, query: str) -> list[str]:
        response = requests.get(
            CLICKHOUSE_URL,
            params={
                "user": CLICKHOUSE_USER,
                "password": CLICKHOUSE_PASSWORD,
                "query": query,
            },
            timeout=10,
        )
        response.raise_for_status()
        return response.text.strip().splitlines() if response.text.strip() else []

    def test_all_cli_commands_end_to_end(self):
        model_name = f"e2e_{uuid.uuid4().hex[:8]}"
        schema = {"event_id": "String", "timestamp": "UInt32", "amount": "Float64"}
        model_id = None

        with tempfile.TemporaryDirectory() as tmpdir:
            schema_path = self._write_file(
                tmpdir,
                "schema.json",
                json.dumps(schema, separators=(",", ":")),
            )
            updated_schema_path = self._write_file(
                tmpdir,
                "updated-schema.json",
                json.dumps(schema, indent=2, sort_keys=True),
            )
            valid_csv_path = self._write_file(
                tmpdir,
                "valid.csv",
                "event_id,timestamp,amount\n1,1716045542,12.5\n2,1716045543,8.0\n",
            )
            partial_csv_path = self._write_file(
                tmpdir,
                "partial.csv",
                "event_id,timestamp,amount\n3,1716045544,9.1\n4,bad,\n",
            )

            try:
                initial_models = self._literal_output("model", "list")
                self.assertIsInstance(initial_models, list)

                created_model = self._literal_output("model", "create", model_name, schema_path)
                model_id = created_model["id"]
                self.assertEqual(created_model["name"], model_name)

                listed_models = self._literal_output("model", "list")
                self.assertIn(model_id, {model["id"] for model in listed_models})

                described_model = self._literal_output("model", "describe", model_id)
                self.assertEqual(described_model["id"], model_id)
                self.assertEqual(described_model["schema"], json.dumps(schema, separators=(",", ":")))

                updated_model = self._literal_output("model", "update", model_id, updated_schema_path)
                self.assertEqual(
                    updated_model["schema"],
                    json.dumps(schema, indent=2, sort_keys=True),
                )

                initial_job_list = self._run_cli("job", "list")
                self.assertIn("JOB", initial_job_list)

                valid_create_output = self._run_cli("job", "create", model_id, valid_csv_path)
                valid_job_match = re.search(r"job ([0-9a-f]+) created\.", valid_create_output)
                self.assertIsNotNone(valid_job_match)
                valid_job_id = valid_job_match.group(1)

                valid_status_output = self._wait_for_job_state(valid_job_id, "SUCCESS")
                self.assertIn(valid_job_id, valid_status_output)

                job_list_after_success = self._run_cli("job", "list")
                self.assertIn(valid_job_id, job_list_after_success)
                self.assertIn("SUCCESS", job_list_after_success)

                inserted_rows = self._query_clickhouse(
                    f"SELECT event_id,timestamp,amount FROM data_{model_id} ORDER BY event_id FORMAT TabSeparated"
                )
                self.assertEqual(
                    inserted_rows,
                    ["1\t1716045542\t12.5", "2\t1716045543\t8"],
                )

                partial_create_output = self._run_cli("job", "create", model_id, partial_csv_path)
                partial_job_match = re.search(r"job ([0-9a-f]+) created\.", partial_create_output)
                self.assertIsNotNone(partial_job_match)
                partial_job_id = partial_job_match.group(1)

                partial_status_output = self._wait_for_job_state(partial_job_id, "PARTIAL_SUCCESS")
                self.assertIn(partial_job_id, partial_status_output)

                rejected_output = self._run_cli("job", "rejected", partial_job_id)
                self.assertIn("ROW,EVENT_ID,COLUMN,TYPE,ERROR,OBSERVED,MESSAGE", rejected_output)
                self.assertIn("INVALID_UINT32", rejected_output)
                self.assertIn("MISSING_COLUMN", rejected_output)

                rejected_rows = self._query_clickhouse(
                    "SELECT job_id,row,event_id,column,error "
                    f"FROM rejected_rows WHERE job_id='{partial_job_id}' "
                    "ORDER BY row, column FORMAT TabSeparated"
                )
                self.assertEqual(
                    rejected_rows,
                    [
                        f"{partial_job_id}\t2\t4\tamount\tMISSING_COLUMN",
                        f"{partial_job_id}\t2\t4\ttimestamp\tINVALID_UINT32",
                    ],
                )

                cancel_create_output = self._run_cli("job", "create", model_id, valid_csv_path)
                cancel_job_match = re.search(r"job ([0-9a-f]+) created\.", cancel_create_output)
                self.assertIsNotNone(cancel_job_match)
                cancel_job_id = cancel_job_match.group(1)

                cancel_output = self._run_cli("job", "cancel", cancel_job_id)
                self.assertEqual(cancel_output, f"job {cancel_job_id} cancelled")

                cancel_status_output = self._wait_for_job_state(cancel_job_id, "CANCELLED")
                self.assertIn(cancel_job_id, cancel_status_output)

                time.sleep(3)
                row_count_after_cancel = self._query_clickhouse(
                    f"SELECT count() FROM data_{model_id} FORMAT TabSeparated"
                )
                self.assertEqual(row_count_after_cancel, ["3"])

                final_job_list = self._run_cli("job", "list")
                self.assertIn(valid_job_id, final_job_list)
                self.assertIn(partial_job_id, final_job_list)
                self.assertIn(cancel_job_id, final_job_list)
                self.assertIn("CANCELLED", final_job_list)
                self.assertIn("PARTIAL_SUCCESS", final_job_list)
                self.assertIn("SUCCESS", final_job_list)

                delete_output = self._run_cli("model", "delete", model_id)
                self.assertEqual(delete_output, "204")
                model_id = None

                remaining_models = self._literal_output("model", "list")
                self.assertNotIn(model_name, {model["name"] for model in remaining_models})
            finally:
                if model_id:
                    try:
                        requests.delete(f"{API_URL}/models/{model_id}", timeout=5)
                    except Exception:
                        pass
