# ClickHouse Tables

Validated rows are stored in ClickHouse for analytical queries.

The provided `docker-compose.yml` includes a `clickhouse` service using the latest image.
When the stack is started locally, the database is reachable at `http://localhost:8123`.
Use `default` / `batchlocal` when querying it from the host.

```sql
-- Example per-model table
CREATE TABLE data_<model_id> (
    event_id String,
    timestamp UInt32,
    amount Float64
) ENGINE = MergeTree
ORDER BY (event_id);

-- Rejected rows across all jobs
CREATE TABLE rejected_rows (
    job_id String,
    row UInt32,
    event_id String,
    column String,
    type String,
    error String,
    observed String,
    message String,
    ts DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (job_id, row);
```

Notes:

- Per-model tables are created lazily by the worker the first time accepted rows are written.
- The ordering key is `event_id` when that column exists; otherwise the first schema column is used.
- Rejected rows are written one record per validation error, so a single source row can produce multiple entries.
