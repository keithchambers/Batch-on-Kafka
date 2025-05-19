# Batch-on-Kafka

This repository contains a proof of concept for batch ingestion backed by **Redpanda** and **ClickHouse**. Uploads are sent through a FastAPI service, written to Redpanda, validated by a worker, and stored in ClickHouse.

## Installation

This project requires **Python&nbsp;3.11**. Install the runtime dependencies
into a virtual environment and expose the CLI:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

The `batch` command is now available and can also be invoked with
`python -m batch`.

## Quick Start

Follow these steps to see the system handle a successful upload (the "happy"
path) and a file with bad data that results in rejected rows.

### 1. Start the stack

```bash
docker-compose up --build
```

The API will be available at `http://localhost:8000`, Redpanda at `localhost:9092` and ClickHouse at `http://localhost:8123`.

### 2. Register a model

Create a simple JSON schema and save it as `model.json`:

```json
{"event_id": "String", "timestamp": "UInt32", "amount": "Float64"}
```

Register the model and note the returned `model_id`:

```bash
batch model create purchases model.json
```

Sample output:

```text
{"id": "a1b2c3d4", "name": "purchases", "schema": "{\"event_id\": \"String\", \"timestamp\": \"UInt32\", \"amount\": \"Float64\"}"}
```

### 3. Ingest valid data

Prepare a CSV file that matches the schema, e.g. `data.csv`:

```csv
event_id,timestamp,amount
1,1716045542,12.5
2,1716045543,8.0
```

Create a job for the file:

```bash
batch job create <model_id> data.csv
```

Sample output:

```text
job e5f6a7b8 created.
```

Check progress until the job state becomes `SUCCESS`:

```bash
batch job status <job_id>
```

Sample output:

```text
JOB      MODEL       STATE           TOTAL   OK      ERRORS  PROGRESS      WAITING  PROCESSSING
abcd1234 purchases   SUCCESS               2       2       0 [#################] 100%   00:00        00:02
```

Query ClickHouse to view the inserted rows:

```bash
curl "http://localhost:8123/?query=SELECT%20*%20FROM%20data_<model_id>"
```

Sample output:

```text
1\t1716045542\t12.5
2\t1716045543\t8.0
```

### 4. Rejected data path

Upload a CSV containing invalid rows, e.g. `bad.csv` with missing or malformed values:

```bash
batch job create <model_id> bad.csv
```

Sample output:

```text
job b7c8d9e0 created.
```

When the job state is `PARTIAL_SUCCESS` or `FAILED`, inspect the problems:

```bash
batch job rejected <job_id>
```

Sample output:

```text
ROW,EVENT_ID,COLUMN,TYPE,ERROR,OBSERVED,MESSAGE
1,1001abcd,timestamp,TIMESTAMP,MISSING_COLUMN,,"The required column 'timestamp' is missing"
```

Rejected rows are also stored in ClickHouse and can be queried directly:

```bash
curl "http://localhost:8123/?query=SELECT%20*%20FROM%20rejected_rows%20WHERE%20job_id='<job_id>'"
```

Sample output:

```text
<job_id>\t1001abcd\ttimestamp\tMISSING_COLUMN
```

## Local Development

Use the CLI via `batch` or `python -m batch`. Set environment variables if running the services elsewhere:

```bash
export BATCH_API_URL=http://localhost:8000
export KAFKA_BOOTSTRAP=localhost:9092
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export METRICS_PORT=9090  # worker metrics
```

Start the services locally without Docker using:

```bash
uvicorn batch.api:app --reload  # API on port 8000
python -m batch.worker          # background worker
```

### Linting and Tests

Install development dependencies and run the linter and test suite:

```bash
pip install ruff
ruff batch tests
python -m unittest discover -s tests
```

See [docsite](./docsite) for detailed API and schema information.

## Monitoring

Prometheus and Grafana are included in `docker-compose.yml`.
Start the full stack with monitoring using:

```bash
docker-compose up --build
```

Prometheus will be available at `http://localhost:9091` and Grafana at `http://localhost:3000`.
The API exposes metrics at `/metrics` and the worker on port `9090`.

