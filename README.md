# Batch-on-Kafka

This repository contains a proof of concept for batch ingestion backed by **Redpanda** and **ClickHouse**. Uploads are sent through a FastAPI service, written to Redpanda, validated by a worker, and stored in ClickHouse.

See [docsite](./docsite) for detailed API and schema information.
