# Message Schemas

## Job Status (`batch.jobs`)
```json
{
  "job_id": "a1b2c3d4",
  "model_id": "fraud_detector",
  "state": "RUNNING|SUCCESS|PARTIAL_SUCCESS|FAILED|PENDING|CANCELLED",
  "totals": {"rows": 50000, "ok": 30500, "errors": 12},
  "timings": {"waiting_ms": 2100, "processing_ms": 40000},
  "updated_at": "2025-05-18T15:05:07Z"
}
```

## Data Row (`/batch/<job_id>`)
```json
{"offset": 0, "payload": "<raw-csv-or-parquet-row-as-bytes>"}
```

## Dead-Letter Row (`/batch/<job_id>/dlq`)
```json
{
  "row": 1,
  "event_id": "1003cdef",
  "column": "timestamp",
  "type": "TIMESTAMP",
  "error": "INVALID_TIMESTAMP",
  "observed": "\"yesterday\"",
  "message": "The 'timestamp' value is invalid..."
}
```
