# Metrics

The API service and worker expose Prometheus metrics.

- **API**: `http://localhost:8000/metrics`
- **Worker**: `http://localhost:9090/metrics`

Prometheus scrapes these endpoints and Grafana visualizes key stats like jobs created and rows inserted.
