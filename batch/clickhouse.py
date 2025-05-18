import os
from typing import Dict, Any, Iterable

import clickhouse_connect

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8123))

_client = None

def get_client():
    global _client
    if _client is None:
        _client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    return _client


def ensure_table(model_id: str, columns: Dict[str, str]) -> None:
    """Create per-model table if it does not already exist."""
    cols = ", ".join(f"{name} {dtype}" for name, dtype in columns.items())
    ddl = (
        f"CREATE TABLE IF NOT EXISTS data_{model_id} ({cols}) "
        "ENGINE = MergeTree ORDER BY (event_id)"
    )
    client = get_client()
    client.command(ddl)


def insert_rows(model_id: str, rows: Iterable[Dict[str, Any]]) -> None:
    rows_list = list(rows)
    if not rows_list:
        return
    columns = rows_list[0].keys()
    client = get_client()
    client.insert(f"data_{model_id}", rows_list, column_names=list(columns))
