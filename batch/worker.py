import asyncio
import base64
import csv
import io
import json
import os
import time
from typing import Any, Dict, List, Tuple

import pyarrow.parquet as pq
import requests
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from . import clickhouse
from .log_config import configure_logging

logger = configure_logging(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
API_URL = os.environ.get("BATCH_API_URL", "http://api:8000")
CANCELLATION_GRACE_MS = int(os.environ.get("BATCH_CANCELLATION_GRACE_MS", "500"))
JOB_UPDATE_RETRIES = int(os.environ.get("BATCH_JOB_UPDATE_RETRIES", "3"))
JOB_UPDATE_RETRY_DELAY_MS = int(os.environ.get("BATCH_JOB_UPDATE_RETRY_DELAY_MS", "500"))
WORKER_GROUP_ID = "batch-worker"
JOB_TOPIC_PATTERN = r"^batch\.[^.]+$"


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
    last_error: Exception | None = None
    for attempt in range(1, JOB_UPDATE_RETRIES + 1):
        try:
            response = requests.put(f"{API_URL}/internal/jobs/{job_id}", json=payload, timeout=10)
            response.raise_for_status()
            return
        except Exception as exc:
            last_error = exc
            if attempt == JOB_UPDATE_RETRIES:
                break
            logger.warning(
                "job_status_update_retry job_id=%s attempt=%d max_attempts=%d error=%s",
                job_id,
                attempt,
                JOB_UPDATE_RETRIES,
                exc,
            )
            time.sleep(JOB_UPDATE_RETRY_DELAY_MS / 1000)

    if last_error is not None:
        raise last_error


def _get_job_state(job_id: str) -> str:
    response = requests.get(f"{API_URL}/jobs/{job_id}", timeout=10)
    response.raise_for_status()
    return str(response.json().get("state", ""))


def _job_is_cancelled(job_id: str) -> bool:
    return _get_job_state(job_id) == "CANCELLED"


def _job_is_cancelled_or_false(job_id: str) -> bool:
    try:
        return _job_is_cancelled(job_id)
    except Exception:
        logger.exception("job_state_lookup_failed job_id=%s", job_id)
        return False


def _processing_delay_ms(created_at_ms: int, now_ms: int) -> int:
    ready_at_ms = created_at_ms + CANCELLATION_GRACE_MS
    return max(0, ready_at_ms - now_ms)


async def _start(client) -> None:
    while True:
        try:
            await client.start()
            logger.info(
                "kafka_client_ready client=%s bootstrap_servers=%s",
                type(client).__name__,
                KAFKA_BOOTSTRAP,
            )
            break
        except Exception as exc:
            logger.warning(
                "kafka_client_connect_failed client=%s bootstrap_servers=%s error=%s",
                type(client).__name__,
                KAFKA_BOOTSTRAP,
                exc,
            )
            await asyncio.sleep(5)


async def consume() -> None:
    """Consume all job topics and insert rows into ClickHouse."""
    logger.info(
        "worker_starting kafka_bootstrap=%s api_url=%s group_id=%s cancellation_grace_ms=%d",
        KAFKA_BOOTSTRAP,
        API_URL,
        WORKER_GROUP_ID,
        CANCELLATION_GRACE_MS,
    )
    consumer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=WORKER_GROUP_ID,
        auto_offset_reset="earliest",
        metadata_max_age_ms=1000,
    )
    consumer.subscribe(pattern=JOB_TOPIC_PATTERN)
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await _start(consumer)
    await _start(producer)
    logger.info("worker_subscribed topic_pattern=%s", JOB_TOPIC_PATTERN)

    try:
        async for msg in consumer:
            job_id = ""
            model_id = ""
            started_at_ms = int(time.time() * 1000)
            persisted = False
            try:
                payload = _decode_job_message(msg.value)
                job_id = payload["job_id"]
                model_id = payload["model_id"]
                filename = payload["filename"]
                logger.info(
                    "job_received job_id=%s model_id=%s topic=%s partition=%s offset=%s filename=%s",
                    job_id,
                    model_id,
                    msg.topic,
                    msg.partition,
                    msg.offset,
                    filename,
                )
                created_at_ms = int(payload.get("created_at_ms", started_at_ms))
                delay_ms = _processing_delay_ms(created_at_ms, started_at_ms)
                if delay_ms:
                    logger.info("job_waiting job_id=%s delay_ms=%d", job_id, delay_ms)
                    await asyncio.sleep(delay_ms / 1000)
                if await asyncio.to_thread(_job_is_cancelled, job_id):
                    logger.info("job_skipped_cancelled job_id=%s phase=preprocess", job_id)
                    continue
                schema = _load_schema(payload["schema"])
                rows = _parse_rows(payload["file_bytes"], filename)
                processing_started_at_ms = int(time.time() * 1000)
                waiting_ms = max(0, processing_started_at_ms - created_at_ms)
                valid_rows, rejected_rows, invalid_row_count = _validate_rows(rows, schema)
                logger.info(
                    "job_validated job_id=%s model_id=%s total_rows=%d valid_rows=%d invalid_rows=%d",
                    job_id,
                    model_id,
                    len(rows),
                    len(valid_rows),
                    invalid_row_count,
                )

                if await asyncio.to_thread(_job_is_cancelled, job_id):
                    logger.info("job_skipped_cancelled job_id=%s phase=persistence", job_id)
                    continue

                if valid_rows:
                    clickhouse.ensure_table(model_id, schema)
                    clickhouse.insert_rows(model_id, valid_rows)
                if rejected_rows:
                    clickhouse.ensure_rejected_table()
                    clickhouse.insert_rejected_rows(job_id, rejected_rows)
                persisted = True
                logger.info(
                    "job_persisted job_id=%s model_id=%s inserted_rows=%d rejected_records=%d",
                    job_id,
                    model_id,
                    len(valid_rows),
                    len(rejected_rows),
                )

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
                logger.info(
                    "job_completed job_id=%s model_id=%s state=%s total_rows=%d ok_rows=%d error_rows=%d waiting_ms=%d processing_ms=%d",
                    job_id,
                    model_id,
                    state,
                    len(rows),
                    len(valid_rows),
                    invalid_row_count,
                    waiting_ms,
                    processing_ms,
                )
            except requests.RequestException:
                logger.exception(
                    "job_status_sync_failed job_id=%s model_id=%s persisted=%s",
                    job_id or "unknown",
                    model_id or "unknown",
                    persisted,
                )
                continue
            except Exception as exc:
                logger.exception(
                    "job_processing_failed job_id=%s topic=%s partition=%s offset=%s",
                    job_id or "unknown",
                    msg.topic,
                    msg.partition,
                    msg.offset,
                )
                if job_id:
                    if await asyncio.to_thread(_job_is_cancelled_or_false, job_id):
                        logger.info("job_skipped_cancelled job_id=%s phase=failure", job_id)
                        continue
                    processing_ms = max(0, int(time.time() * 1000) - started_at_ms)
                    dlq_topic = f"batch.{job_id}.dlq"
                    await producer.send_and_wait(dlq_topic, msg.value)
                    logger.info("job_sent_to_dlq job_id=%s topic=%s", job_id, dlq_topic)
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
                        logger.exception("job_status_update_failed job_id=%s state=FAILED", job_id)
    finally:
        await consumer.stop()
        await producer.stop()
        logger.info("worker_stopped")


def main() -> None:
    """Entry point for the worker."""
    asyncio.run(consume())


if __name__ == "__main__":
    main()
