# Batch Ingestion Proof of Concept

This documentation describes the PoC for batching data into analytics storage using Kafka-compatible Redpanda and ClickHouse.

## Overview

- **Redpanda** provides the Kafka-compatible broker used for per-job ingestion payloads and dead-letter messages.
- **ClickHouse** stores validated rows in per-model tables plus rejected row details in a shared table.
- The API service accepts file uploads, tracks model and job state in memory, and publishes one Kafka message per submitted job.
- A background worker subscribes to job topics, validates rows against the submitted schema, writes accepted rows to ClickHouse, and reports job status back to the API.

See the other pages in this documentation for CLI usage, REST API details, Kafka topics, and message schemas.
