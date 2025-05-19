# Batch-on-Kafka

This repository contains a proof of concept for batch ingestion backed by **Redpanda** and **ClickHouse**. Uploads are sent through a FastAPI service, written to Redpanda, validated by a worker, and stored in ClickHouse.

## Quick Start

Follow these steps to see the system handle a successful upload (the "happy path") and a file with bad data that results in rejected rows.

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
python -m batch.cli model create purchases model.json
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
python -m batch.cli job create <model_id> data.csv
```

Check progress until the job state becomes `SUCCESS`:

```bash
python -m batch.cli job status <job_id>
```

Query ClickHouse to view the inserted rows:

```bash
curl "http://localhost:8123/?query=SELECT%20*%20FROM%20data_<model_id>"
```

### 4. Rejected data path

Upload a CSV containing invalid rows, e.g. `bad.csv` with missing or malformed values:

```bash
python -m batch.cli job create <model_id> bad.csv
```

When the job state is `PARTIAL_SUCCESS` or `FAILED`, inspect the problems:

```bash
python -m batch.cli job rejected <job_id>
```

Rejected rows are also stored in ClickHouse and can be queried directly:

```bash
curl "http://localhost:8123/?query=SELECT%20*%20FROM%20rejected_rows%20WHERE%20job_id='<job_id>'"
```

## Local Development

Use the CLI via `python -m batch.cli`. Set `BATCH_API_URL` and `KAFKA_BOOTSTRAP` if running the services elsewhere:

```bash
export BATCH_API_URL=http://localhost:8000
export KAFKA_BOOTSTRAP=localhost:9092
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

