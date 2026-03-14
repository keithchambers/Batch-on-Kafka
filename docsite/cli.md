# Command-Line Interface (CLI)

Run commands via `batch <command>` after `pip install -e .`, or use `python -m batch <command>`.

If the API is not running at `http://localhost:8000`, set:

```bash
export BATCH_API_URL=http://localhost:8000
export BATCH_API_TIMEOUT_SECONDS=30
```

The CLI prints JSON for model commands, table output for job status/list commands, and CSV for rejected rows. API failures exit non-zero and print the error message to stderr.

## Model Commands

### `model list`
```bash
batch model list
```

Sample output:

```text
[{"id": "a1b2c3d4", "name": "purchases", "schema": "{\"event_id\":\"String\",\"timestamp\":\"UInt32\",\"amount\":\"Float64\"}"}]
```

### `model describe <model_id>`
```bash
batch model describe <model_id>
```

### `model create <name> <schema.json>`
```bash
batch model create purchases model.json
```

Sample output:

```text
{"id": "a1b2c3d4", "name": "purchases", "schema": "{\"event_id\":\"String\",\"timestamp\":\"UInt32\",\"amount\":\"Float64\"}"}
```

### `model update <model_id> <schema.json>`
```bash
batch model update <model_id> model.json
```

### `model delete <model_id>`
```bash
batch model delete <model_id>
```

Sample output:

```text
204
```

## Job Commands

### `job list`
```bash
batch job list
```

Sample output:

```text
JOB      MODEL       STATE           TOTAL   OK      ERRORS  PROGRESS      WAITING  PROCESSING
a1b2c3d4 a1b2c3d4    SUCCESS               2       2       0 [#################] 100%   00:00        00:01
b2c3d4e5 a1b2c3d4    PARTIAL_SUCCESS       2       1       1 [########--------]  50%   00:00        00:01
c3d4e5f6 a1b2c3d4    CANCELLED             1       0       0 [XXXXXXXXXXXXXXXXX]   0%   00:00        00:00
```

### `job create <model_id> <data.csv|data.parquet>`
```bash
batch job create <model_id> data.csv
```

Sample output:

```text
job e5f6a7b8 created.
```

### `job status <job_id>`
```bash
batch job status <job_id>
```

Sample output:

```text
JOB      MODEL       STATE           TOTAL   OK      ERRORS  PROGRESS      WAITING  PROCESSING
e5f6a7b8 a1b2c3d4    SUCCESS               2       2       0 [#################] 100%   00:00        00:01
```

### `job cancel <job_id>`
```bash
batch job cancel <job_id>
```

Sample output:

```text
job e5f6a7b8 cancelled
```

### `job rejected <job_id>`
```bash
batch job rejected <job_id>
```

Sample output:

```text
ROW,EVENT_ID,COLUMN,TYPE,ERROR,OBSERVED,MESSAGE
2,4,amount,Float64,MISSING_COLUMN,,"The required column 'amount' is missing"
2,4,timestamp,UInt32,INVALID_UINT32,bad,The value for 'timestamp' must be an unsigned 32-bit integer
```
