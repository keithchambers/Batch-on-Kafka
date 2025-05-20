# Batch Ingestion Proof of Concept

This documentation describes the PoC for batching data into analytics storage using Kafka-compatible Redpanda and ClickHouse.

## Overview

- **Redpanda** provides the Kafka API broker used for job data and model metadata.
- **ClickHouse** stores validated rows and rejected rows for analytical queries.
- The API service accepts file uploads and streams rows to Redpanda.
- A background worker consumes each job's topic, validates rows against the registered model schema, and inserts good rows into ClickHouse.

See the other pages in this documentation for CLI usage, REST API details, Kafka topics, and message schemas.
