# Kafka Topics

The system uses Redpanda as the Kafka broker. Topics are created per job.

| Purpose | Name | Producer | Consumer |
|---------|------|----------|----------|
| Job payload | `batch.<job_id>` | API | Worker subscription pattern `^batch\.[^.]+$` |
| Dead-letter payload | `batch.<job_id>.dlq` | Worker | none in this repository |

The application does not publish model metadata or job status events to shared Kafka topics. Job status lives in the API process and rejected row details are stored in ClickHouse.
