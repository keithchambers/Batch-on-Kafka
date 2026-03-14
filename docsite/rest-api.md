# REST API

Public endpoints return JSON except the rejected-row download, which returns `text/csv`. Success is indicated by 2xx status codes.

| Method | Path | Purpose | Success Code |
|--------|------|---------|--------------|
| `GET` | `/models` | List models | `200` |
| `GET` | `/models/{id}` | Fetch one model | `200` |
| `POST` | `/models` | Create model. Body: `{"name": "...", "schema": "..."}` | `201` |
| `PUT` | `/models/{id}` | Update model fields. Typically `schema` | `200` |
| `DELETE` | `/models/{id}` | Delete model | `204` |
| `POST` | `/jobs?model_id={id}` | Multipart upload with `file` part | `202` |
| `GET` | `/jobs` | List jobs | `200` |
| `GET` | `/jobs/{id}` | Fetch one job | `200` |
| `DELETE` | `/jobs/{id}` | Mark a job cancelled | `202` |
| `GET` | `/jobs/{id}/rejected` | Download rejected rows as CSV | `200` |

Uploads must be `.csv` or `.parquet`. Files larger than 1&nbsp;GB are rejected.

## Model Payload

```json
{
  "id": "a1b2c3d4",
  "name": "purchases",
  "schema": "{\"event_id\":\"String\",\"timestamp\":\"UInt32\",\"amount\":\"Float64\"}"
}
```

## Job Payload

```json
{
  "id": "e5f6a7b8",
  "model_id": "a1b2c3d4",
  "state": "SUCCESS",
  "totals": {
    "rows": 2,
    "ok": 2,
    "errors": 0
  },
  "timings": {
    "waiting_ms": 0,
    "processing_ms": 12
  },
  "rejected_rows": [],
  "created_at_ms": 1773466325123
}
```

## Internal Worker Endpoint

The worker updates job state through:

| Method | Path | Purpose |
|--------|------|---------|
| `PUT` | `/internal/jobs/{id}` | Replace job state, totals, timings, rejected rows, and optional error detail |
