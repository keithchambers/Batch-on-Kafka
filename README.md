# Batch-on-Kafka

This repository contains a proof of concept for batch ingestion backed by **Redpanda** and **ClickHouse**. Uploads are sent through a FastAPI service, written to Redpanda, validated by a worker, and stored in ClickHouse.

See [docsite](./docsite) for detailed API and schema information.

## Local Development

Build and start the stack using Docker Compose:

```bash
docker-compose up --build
```

The API will be available at `http://localhost:8000` and Redpanda at `localhost:9092`.
Use the CLI via `python -m batch.cli`.
Set `BATCH_API_URL` and `KAFKA_BOOTSTRAP` if running the services elsewhere:

```bash
export BATCH_API_URL=http://localhost:8000
export KAFKA_BOOTSTRAP=localhost:9092
```

### Linting and Tests

Install development dependencies and run the linter and test suite:

```bash
pip install ruff pytest
ruff batch tests
pytest
```
