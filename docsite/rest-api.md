# REST API

All endpoints return JSON. Success is indicated by 2xx codes; non-2xx codes contain an error description.

| Method | Path | Purpose | Success Code |
|-------|------|---------|--------------|
| `GET` | `/models` | List models | `200` |
| `POST` | `/models` | Create model. Body: `{\"name\":...,\"schema\":...}` | `201` |
| `PUT` | `/models/{id}` | Update schema | `200` |
| `DELETE` | `/models/{id}` | Delete model | `204` |
| `POST` | `/jobs` | Multipart upload with `model_id` and file | `202` (returns `{job_id}`) |
| `GET` | `/jobs` | List jobs | `200` |
| `GET` | `/jobs/{id}` | Job status | `200` |
| `DELETE` | `/jobs/{id}` | Cancel job | `202` |
| `GET` | `/jobs/{id}/rejected` | Stream rejected rows (CSV) | `200` |

Uploads must be `.csv` or `.parquet`. Files larger than 1&nbsp;GB are rejected.
