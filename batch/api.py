from fastapi import FastAPI, UploadFile, File, HTTPException, status
from fastapi.responses import JSONResponse, StreamingResponse
from uuid import uuid4
import os
import csv
import asyncio
from typing import Dict, Any, Optional

import pyarrow.parquet as pq
from aiokafka import AIOKafkaProducer

app = FastAPI()

MODELS: Dict[str, Dict[str, Any]] = {}
JOBS: Dict[str, Dict[str, Any]] = {}
MAX_FILE_SIZE = 1 * 1024 * 1024 * 1024  # 1GB
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
_producer: Optional[AIOKafkaProducer] = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    while True:
        try:
            await _producer.start()
            break
        except Exception as exc:
            # keep retrying until Redpanda is available
            print(f"Producer connect failed: {exc}. Retrying...")
            await asyncio.sleep(5)
    return _producer


def _peek_validate(file_path: str, ext: str):
    """Peek into the file to ensure it's a valid CSV or Parquet."""
    if ext == ".csv":
        with open(file_path, "rb") as f:
            head = f.read(1024)
        try:
            head.decode("utf-8")
        except UnicodeDecodeError as e:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, detail=f"Invalid CSV encoding: {e}"
            )
        try:
            sample = head.decode("utf-8").splitlines()[0]
            csv.reader([sample])
        except Exception as e:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, detail=f"Invalid CSV content: {e}"
            )
    elif ext == ".parquet":
        try:
            pq.ParquetFile(file_path)
        except Exception as e:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, detail=f"Invalid Parquet file: {e}"
            )
    else:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Unsupported file type")


@app.get("/models")
def list_models():
    return list(MODELS.values())


@app.post("/models", status_code=status.HTTP_201_CREATED)
def create_model(model: Dict[str, Any]):
    model_id = uuid4().hex[:8]
    MODELS[model_id] = {"id": model_id, **model}
    return MODELS[model_id]


@app.put("/models/{model_id}")
def update_model(model_id: str, model: Dict[str, Any]):
    if model_id not in MODELS:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Model not found")
    MODELS[model_id].update(model)
    return MODELS[model_id]


@app.delete("/models/{model_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_model(model_id: str):
    if MODELS.pop(model_id, None) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Model not found")
    return JSONResponse(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/jobs", status_code=status.HTTP_202_ACCEPTED)
async def create_job(model_id: str, file: UploadFile = File(...)):
    if model_id not in MODELS:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Model not found")

    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in {".csv", ".parquet"}:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, detail="File must be CSV or Parquet"
        )

    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(
            status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="File exceeds 1GB limit"
        )

    tmp_path = f"/tmp/{uuid4().hex}{ext}"
    with open(tmp_path, "wb") as f:
        f.write(contents)

    _peek_validate(tmp_path, ext)

    producer = await get_producer()
    job_id = uuid4().hex[:8]
    topic = f"/batch/{job_id}"
    await producer.send_and_wait(topic, contents)
    os.remove(tmp_path)

    JOBS[job_id] = {
        "id": job_id,
        "model_id": model_id,
        "state": "PENDING",
        "totals": {"rows": 0, "ok": 0, "errors": 0},
    }
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


@app.delete("/jobs/{job_id}", status_code=status.HTTP_202_ACCEPTED)
def cancel_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Job not found")
    job["state"] = "CANCELLED"
    return {"detail": f"job {job_id} cancelled"}


@app.get("/jobs/{job_id}/rejected")
def rejected_rows(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Job not found")

    async def iterator():
        yield "ROW,EVENT_ID,COLUMN,TYPE,ERROR,OBSERVED,MESSAGE\n"

    return StreamingResponse(iterator(), media_type="text/csv")
