# Kafka Topics

The system uses Redpanda as the Kafka broker. Topics are created dynamically per job and retain data for a limited time.

| Purpose | Name | Partitions | Cleanup | Retention |
|---------|------|-----------|---------|-----------|
| Job metadata | `batch.jobs` | 3 | compact | infinite |
| Per-job data | `/batch/<job_id>` | 6 | delete | 1 day (configurable via `RETENTION_HOURS`) |
| Per-job dead-letter | `/batch/<job_id>/dlq` | 6 | delete | 1 day (same as above) |

A compacted topic `batch.models` stores model schemas so both services can reload them on startup.
