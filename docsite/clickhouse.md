# ClickHouse Tables

Validated rows are stored in ClickHouse for analytical queries.

```sql
-- Example per-model table
CREATE TABLE data_<model_id> (
    event_id String,
    timestamp UInt32,
    -- additional attributes depending on the model schema
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
