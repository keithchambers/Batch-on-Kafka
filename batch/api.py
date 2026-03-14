import asyncio
import base64
import csv
import io
import json
import os
import time
from typing import Any, Dict, Optional
from uuid import uuid4

import pyarrow.parquet as pq
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, File, HTTPException, Response, UploadFile, status
from fastapi.responses import StreamingResponse

from .log_config import configure_logging

app = FastAPI()

MODELS: Dict[str, Dict[str, Any]] = {}
JOBS: Dict[str, Dict[str, Any]] = {}
MAX_FILE_SIZE = 1 * 1024 * 1024 * 1024  # 1GB
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
_producer: Optional[AIOKafkaProducer] = None
_producer_started = False
_producer_lock: Optional[asyncio.Lock] = None

logger = configure_logging(__name__)


def _job_topic(job_id: str) -> str:
    return f"batch.{job_id}"


def _encode_job_message(
    job_id: str,
    model_id: str,
    schema: str,
    filename: str,
    contents: bytes,
    created_at_ms: int,
) -> bytes:
    return json.dumps(
        {
            "job_id": job_id,
            "model_id": model_id,
            "schema": schema,
            "filename": filename,
            "created_at_ms": created_at_ms,
            "payload_b64": base64.b64encode(contents).decode("ascii"),
        }
    ).encode("utf-8")


async def get_producer() -> AIOKafkaProducer:
    global _producer, _producer_started, _producer_lock
    if _producer is None:
        _producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    if _producer_lock is None:
        _producer_lock = asyncio.Lock()
    if _producer_started:
        return _producer

    async with _producer_lock:
        if _producer_started:
            return _producer
        while True:
            try:
                await _producer.start()
                _producer_started = True
                logger.info("kafka_producer_ready bootstrap_servers=%s", KAFKA_BOOTSTRAP)
                break
            except Exception as exc:
                logger.warning(
                    "kafka_producer_connect_failed bootstrap_servers=%s error=%s",
                    KAFKA_BOOTSTRAP,
                    exc,
                )
                await asyncio.sleep(5)
    return _producer


@app.on_event("shutdown")
async def shutdown_producer() -> None:
    global _producer_started
    if _producer is not None and _producer_started:
        await _producer.stop()
        _producer_started = False
        logger.info("kafka_producer_stopped")


def _validate_schema_text(schema_text: str) -> str:
    try:
        schema = json.loads(schema_text)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Schema must be a non-empty JSON object",
        ) from exc

    if not isinstance(schema, dict) or not schema:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Schema must be a non-empty JSON object",
        )

    return schema_text


def _peek_validate(file_path: str, ext: str) -> None:
    """Peek into the file to ensure it's a valid CSV or Parquet."""
    if ext == ".csv":
        with open(file_path, "rb") as f:
            head = f.read(1024)
        try:
            text = head.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, detail=f"Invalid CSV encoding: {exc}"
            ) from exc
        lines = text.splitlines()
        if not lines:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, detail="Empty CSV file or no newline"
            )
        try:
            csv.reader([lines[0]])
        except Exception as exc:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, detail=f"Invalid CSV content: {exc}"
            ) from exc
        return

    if ext == ".parquet":
        try:
            pq.ParquetFile(file_path)
        except Exception as exc:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, detail=f"Invalid Parquet file: {exc}"
            ) from exc
        return

    raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Unsupported file type")


def _rejected_rows_csv(rows: list[Dict[str, Any]]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["ROW", "EVENT_ID", "COLUMN", "TYPE", "ERROR", "OBSERVED", "MESSAGE"])
    for row in rows:
        writer.writerow(
            [
                row.get("row", ""),
                row.get("event_id", ""),
                row.get("column", ""),
                row.get("type", ""),
                row.get("error", ""),
                row.get("observed", ""),
                row.get("message", ""),
            ]
        )
    return buffer.getvalue()


@app.get("/models")
def list_models():
    return list(MODELS.values())


@app.get("/models/{model_id}")
def get_model(model_id: str):
    model = MODELS.get(model_id)
    if not model:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Model not found")
    return model


@app.post("/models", status_code=status.HTTP_201_CREATED)
def create_model(model: Dict[str, Any]):
    model_id = uuid4().hex[:8]
    schema_text = _validate_schema_text(str(model.get("schema", "")))
    MODELS[model_id] = {"id": model_id, **model, "schema": schema_text}
    logger.info(
        "model_created model_id=%s name=%s",
        model_id,
        MODELS[model_id].get("name", ""),
    )
    return MODELS[model_id]


@app.put("/models/{model_id}")
def update_model(model_id: str, model: Dict[str, Any]):
    if model_id not in MODELS:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Model not found")
    if "schema" in model:
        model["schema"] = _validate_schema_text(str(model["schema"]))
    MODELS[model_id].update(model)
    logger.info(
        "model_updated model_id=%s fields=%s",
        model_id,
        ",".join(sorted(model.keys())),
    )
    return MODELS[model_id]


@app.delete("/models/{model_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_model(model_id: str):
    if MODELS.pop(model_id, None) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Model not found")
    logger.info("model_deleted model_id=%s", model_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/jobs", status_code=status.HTTP_202_ACCEPTED)
async def create_job(model_id: str, file: UploadFile = File(...)):
    model = MODELS.get(model_id)
    if model is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Model not found")

    filename = file.filename or "upload"
    ext = os.path.splitext(filename)[1].lower()
    if ext not in {".csv", ".parquet"}:
        logger.warning(
            "job_rejected_invalid_file_type model_id=%s filename=%s ext=%s",
            model_id,
            filename,
            ext,
        )
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, detail="File must be CSV or Parquet"
        )

    logger.info(
        "job_upload_received model_id=%s filename=%s content_type=%s",
        model_id,
        filename,
        file.content_type or "",
    )

    tmp_path = f"/tmp/{uuid4().hex}{ext}"
    size = 0
    with open(tmp_path, "wb") as f:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > MAX_FILE_SIZE:
                f.close()
                os.remove(tmp_path)
                logger.warning(
                    "job_rejected_file_too_large model_id=%s filename=%s size_bytes=%d",
                    model_id,
                    filename,
                    size,
                )
                raise HTTPException(
                    status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail="File exceeds 1GB limit",
                )
            f.write(chunk)

    try:
        _peek_validate(tmp_path, ext)
        with open(tmp_path, "rb") as f:
            contents = f.read()
    except HTTPException as exc:
        os.remove(tmp_path)
        logger.warning(
            "job_rejected_validation_error model_id=%s filename=%s detail=%s",
            model_id,
            filename,
            exc.detail,
        )
        raise

    job_id = uuid4().hex[:8]
    created_at_ms = int(time.time() * 1000)
    JOBS[job_id] = {
        "id": job_id,
        "model_id": model_id,
        "state": "PENDING",
        "totals": {"rows": 0, "ok": 0, "errors": 0},
        "timings": {"waiting_ms": 0, "processing_ms": 0},
        "rejected_rows": [],
        "created_at_ms": created_at_ms,
    }

    producer = await get_producer()
    topic = _job_topic(job_id)
    try:
        await producer.send_and_wait(
            topic,
            _encode_job_message(
                job_id=job_id,
                model_id=model_id,
                schema=model["schema"],
                filename=filename,
                contents=contents,
                created_at_ms=created_at_ms,
            ),
        )
        logger.info(
            "job_enqueued job_id=%s model_id=%s topic=%s filename=%s size_bytes=%d",
            job_id,
            model_id,
            topic,
            filename,
            len(contents),
        )
    except Exception as exc:
        JOBS.pop(job_id, None)
        logger.exception(
            "job_enqueue_failed job_id=%s model_id=%s topic=%s filename=%s",
            job_id,
            model_id,
            topic,
            filename,
        )
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, detail="Unable to enqueue job"
        ) from exc
    finally:
        os.remove(tmp_path)

    return {"job_id": job_id}


@app.get("/jobs")
def list_jobs():
    return list(JOBS.values())


@app.get("/jobs/{job_id}")
def job_status(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Job not found")
    return job


@app.put("/internal/jobs/{job_id}")
def update_job(job_id: str, update: Dict[str, Any]):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Job not found")

    from_state = str(job.get("state", ""))
    if job.get("state") == "CANCELLED":
        logger.info("job_update_ignored job_id=%s state=CANCELLED", job_id)
        return job

    if "state" in update:
        job["state"] = update["state"]
    if "totals" in update:
        job["totals"] = update["totals"]
    if "timings" in update:
        job["timings"] = update["timings"]
    if "rejected_rows" in update:
        job["rejected_rows"] = update["rejected_rows"]
    if "detail" in update:
        job["detail"] = update["detail"]
    logger.info(
        "job_status_updated job_id=%s from_state=%s state=%s rows=%s ok=%s errors=%s",
        job_id,
        from_state,
        job.get("state", ""),
        job.get("totals", {}).get("rows", 0),
        job.get("totals", {}).get("ok", 0),
        job.get("totals", {}).get("errors", 0),
    )
    return job


@app.delete("/jobs/{job_id}", status_code=status.HTTP_202_ACCEPTED)
def cancel_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Job not found")
    job["state"] = "CANCELLED"
    logger.info("job_cancelled job_id=%s", job_id)
    return {"detail": f"job {job_id} cancelled"}


@app.get("/jobs/{job_id}/rejected")
def rejected_rows(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Job not found")

    logger.info(
        "job_rejected_rows_requested job_id=%s rejected_records=%d",
        job_id,
        len(job.get("rejected_rows", [])),
    )
    return StreamingResponse(
        iter([_rejected_rows_csv(job.get("rejected_rows", []))]),
        media_type="text/csv",
    )
