# Message Schemas

## Kafka Job Payload (`batch.<job_id>`)

Each submitted job is encoded as one JSON message:

```json
{
  "job_id": "e5f6a7b8",
  "model_id": "a1b2c3d4",
  "schema": "{\"event_id\":\"String\",\"timestamp\":\"UInt32\",\"amount\":\"Float64\"}",
  "filename": "data.csv",
  "created_at_ms": 1773466325123,
  "payload_b64": "<base64-encoded CSV or Parquet bytes>"
}
```

## Dead-Letter Payload (`batch.<job_id>.dlq`)

The worker writes the original Kafka job payload to the DLQ topic when processing fails before the job can complete successfully.

## Internal Job Update Payload (`PUT /internal/jobs/{job_id}`)

```json
{
  "state": "SUCCESS|PARTIAL_SUCCESS|FAILED",
  "totals": {
    "rows": 2,
    "ok": 1,
    "errors": 1
  },
  "timings": {
    "waiting_ms": 0,
    "processing_ms": 12
  },
  "rejected_rows": [
    {
      "row": 2,
      "event_id": "4",
      "column": "timestamp",
      "type": "UInt32",
      "error": "INVALID_UINT32",
      "observed": "bad",
      "message": "The value for 'timestamp' must be an unsigned 32-bit integer"
    }
  ],
  "detail": "optional failure description"
}
```

## Rejected Row Record

Rejected rows are stored in the API job state and in ClickHouse using this shape:

```json
{
  "row": 2,
  "event_id": "4",
  "column": "amount",
  "type": "Float64",
  "error": "MISSING_COLUMN",
  "observed": "",
  "message": "The required column 'amount' is missing"
}
```
