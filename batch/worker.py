import asyncio
import base64
import csv
import io
import json
import logging
import os
import time
from typing import Any, Dict, List, Tuple

import pyarrow.parquet as pq
import requests
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from . import clickhouse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
API_URL = os.environ.get("BATCH_API_URL", "http://api:8000")
CANCELLATION_GRACE_MS = int(os.environ.get("BATCH_CANCELLATION_GRACE_MS", "500"))


def _parse_rows(data: bytes, filename: str) -> List[Dict[str, Any]]:
    """Return a list of rows parsed from CSV or Parquet bytes."""
    ext = os.path.splitext(filename)[1].lower()
    if ext == ".parquet":
        table = pq.read_table(io.BytesIO(data))
        return [dict(row) for row in table.to_pylist()]

    if ext == ".csv":
        text = data.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in reader]

    raise ValueError(f"Unsupported file type: {ext}")


def _decode_job_message(data: bytes) -> Dict[str, Any]:
    payload = json.loads(data.decode("utf-8"))
    payload["file_bytes"] = base64.b64decode(payload["payload_b64"])
    return payload


def _load_schema(schema_text: str) -> Dict[str, str]:
    schema = json.loads(schema_text)
    if not isinstance(schema, dict) or not schema:
        raise ValueError("Schema must be a non-empty JSON object")
    return {str(key): str(value) for key, value in schema.items()}


def _invalid_value(column: str, dtype: str, value: Any, error: str, message: str) -> Dict[str, str]:
    observed = "" if value is None else str(value)
    return {
        "column": column,
        "type": dtype,
        "error": error,
        "observed": observed,
        "message": message,
    }


def _coerce_row_value(column: str, dtype: str, value: Any) -> Tuple[Any, Dict[str, str] | None]:
    if value in (None, ""):
        return None, _invalid_value(
            column,
            dtype,
            value,
            "MISSING_COLUMN",
            f"The required column '{column}' is missing",
        )

    if dtype == "String":
        return str(value), None

    if dtype == "UInt32":
        try:
            if isinstance(value, float) and not value.is_integer():
                raise ValueError("non-integer float")
            parsed = int(value)
        except (TypeError, ValueError):
            return None, _invalid_value(
                column,
                dtype,
                value,
                "INVALID_UINT32",
                f"The value for '{column}' must be an unsigned 32-bit integer",
            )
        if parsed < 0 or parsed > 4294967295:
            return None, _invalid_value(
                column,
                dtype,
                value,
                "INVALID_UINT32",
                f"The value for '{column}' must be an unsigned 32-bit integer",
            )
        return parsed, None

    if dtype == "Float64":
        try:
            return float(value), None
        except (TypeError, ValueError):
            return None, _invalid_value(
                column,
                dtype,
                value,
                "INVALID_FLOAT64",
                f"The value for '{column}' must be a float",
            )

    return None, _invalid_value(
        column,
        dtype,
        value,
        "UNSUPPORTED_TYPE",
        f"Unsupported schema type '{dtype}'",
    )


def _validate_rows(
    rows: List[Dict[str, Any]], schema: Dict[str, str]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
    valid_rows: List[Dict[str, Any]] = []
    rejected_rows: List[Dict[str, Any]] = []
    invalid_row_count = 0

    for row_number, row in enumerate(rows, start=1):
        typed_row: Dict[str, Any] = {}
        row_errors: List[Dict[str, str]] = []
        event_id = "" if row.get("event_id") in (None, "") else str(row.get("event_id"))

        for column, dtype in schema.items():
            coerced_value, error = _coerce_row_value(column, dtype, row.get(column))
            if error is not None:
                row_errors.append(
                    {
                        "row": row_number,
                        "event_id": event_id,
                        **error,
                    }
                )
                continue
            typed_row[column] = coerced_value

        if row_errors:
            invalid_row_count += 1
            rejected_rows.extend(row_errors)
            continue

        valid_rows.append(typed_row)

    return valid_rows, rejected_rows, invalid_row_count


def _job_update(
    state: str,
    total_rows: int,
    ok_rows: int,
    error_rows: int,
    waiting_ms: int,
    processing_ms: int,
    rejected_rows: List[Dict[str, Any]],
    detail: str | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "state": state,
        "totals": {"rows": total_rows, "ok": ok_rows, "errors": error_rows},
        "timings": {"waiting_ms": waiting_ms, "processing_ms": processing_ms},
        "rejected_rows": rejected_rows,
    }
    if detail:
        payload["detail"] = detail
    return payload


def _update_job(job_id: str, payload: Dict[str, Any]) -> None:
    response = requests.put(f"{API_URL}/internal/jobs/{job_id}", json=payload, timeout=10)
    response.raise_for_status()


def _get_job_state(job_id: str) -> str:
    response = requests.get(f"{API_URL}/jobs/{job_id}", timeout=10)
    response.raise_for_status()
    return str(response.json().get("state", ""))


def _job_is_cancelled(job_id: str) -> bool:
    return _get_job_state(job_id) == "CANCELLED"


def _processing_delay_ms(created_at_ms: int, now_ms: int) -> int:
    ready_at_ms = created_at_ms + CANCELLATION_GRACE_MS
    return max(0, ready_at_ms - now_ms)


async def _start(client) -> None:
    while True:
        try:
            await client.start()
            break
        except Exception as exc:
            logger.warning("Kafka connect failed: %s. Retrying...", exc)
            await asyncio.sleep(5)


async def consume() -> None:
    """Consume all job topics and insert rows into ClickHouse."""
    consumer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="batch-worker",
        auto_offset_reset="earliest",
        metadata_max_age_ms=1000,
    )
    consumer.subscribe(pattern=r"^batch\.[^.]+$")
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await _start(consumer)
    await _start(producer)

    try:
        async for msg in consumer:
            job_id = ""
            started_at_ms = int(time.time() * 1000)
            try:
                payload = _decode_job_message(msg.value)
                job_id = payload["job_id"]
                created_at_ms = int(payload.get("created_at_ms", started_at_ms))
                delay_ms = _processing_delay_ms(created_at_ms, started_at_ms)
                if delay_ms:
                    await asyncio.sleep(delay_ms / 1000)
                if await asyncio.to_thread(_job_is_cancelled, job_id):
                    logger.info("Skipping cancelled job %s before processing", job_id)
                    continue
                model_id = payload["model_id"]
                schema = _load_schema(payload["schema"])
                rows = _parse_rows(payload["file_bytes"], payload["filename"])
                processing_started_at_ms = int(time.time() * 1000)
                waiting_ms = max(
                    0, processing_started_at_ms - created_at_ms
                )
                valid_rows, rejected_rows, invalid_row_count = _validate_rows(rows, schema)

                if await asyncio.to_thread(_job_is_cancelled, job_id):
                    logger.info("Skipping cancelled job %s before persistence", job_id)
                    continue

                if valid_rows:
                    clickhouse.ensure_table(model_id, schema)
                    clickhouse.insert_rows(model_id, valid_rows)
                if rejected_rows:
                    clickhouse.ensure_rejected_table()
                    clickhouse.insert_rejected_rows(job_id, rejected_rows)

                processing_ms = max(0, int(time.time() * 1000) - started_at_ms)
                if invalid_row_count == 0:
                    state = "SUCCESS"
                elif valid_rows:
                    state = "PARTIAL_SUCCESS"
                else:
                    state = "FAILED"

                await asyncio.to_thread(
                    _update_job,
                    job_id,
                    _job_update(
                        state=state,
                        total_rows=len(rows),
                        ok_rows=len(valid_rows),
                        error_rows=invalid_row_count,
                        waiting_ms=waiting_ms,
                        processing_ms=processing_ms,
                        rejected_rows=rejected_rows,
                    ),
                )
                logger.info("Processed job %s with state %s", job_id, state)
            except Exception as exc:
                logger.exception("Processing failed: %s", exc)
                if job_id:
                    if await asyncio.to_thread(_job_is_cancelled, job_id):
                        logger.info("Skipping failure handling for cancelled job %s", job_id)
                        continue
                    processing_ms = max(0, int(time.time() * 1000) - started_at_ms)
                    await producer.send_and_wait(f"batch.{job_id}.dlq", msg.value)
                    try:
                        await asyncio.to_thread(
                            _update_job,
                            job_id,
                            _job_update(
                                state="FAILED",
                                total_rows=0,
                                ok_rows=0,
                                error_rows=0,
                                waiting_ms=0,
                                processing_ms=processing_ms,
                                rejected_rows=[],
                                detail=str(exc),
                            ),
                        )
                    except Exception:
                        logger.exception("Failed to update job %s in API", job_id)
    finally:
        await consumer.stop()
        await producer.stop()


def main() -> None:
    """Entry point for the worker."""
    asyncio.run(consume())


if __name__ == "__main__":
    main()
