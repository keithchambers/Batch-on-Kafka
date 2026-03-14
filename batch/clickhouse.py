import os
from typing import Dict, Any, Iterable

import clickhouse_connect

from .log_config import configure_logging

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

_client = None
logger = configure_logging(__name__)


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _data_table_name(model_id: str) -> str:
    return f"data_{model_id}"


def get_client():
    global _client
    if _client is None:
        kwargs = {
            "host": CLICKHOUSE_HOST,
            "port": CLICKHOUSE_PORT,
            "username": CLICKHOUSE_USER,
        }
        if CLICKHOUSE_PASSWORD:
            kwargs["password"] = CLICKHOUSE_PASSWORD
        _client = clickhouse_connect.get_client(**kwargs)
        logger.info(
            "clickhouse_client_ready host=%s port=%s user=%s",
            CLICKHOUSE_HOST,
            CLICKHOUSE_PORT,
            CLICKHOUSE_USER,
        )
    return _client


def ensure_table(model_id: str, columns: Dict[str, str]) -> None:
    """Create per-model table if it does not already exist."""
    if not columns:
        return
    cols = ", ".join(f"{_quote_identifier(name)} {dtype}" for name, dtype in columns.items())
    if "event_id" in columns:
        order_by = _quote_identifier("event_id")
    else:
        order_by = _quote_identifier(next(iter(columns)))
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {_quote_identifier(_data_table_name(model_id))} ({cols}) "
        f"ENGINE = MergeTree ORDER BY ({order_by})"
    )
    client = get_client()
    client.command(ddl)
    logger.info(
        "clickhouse_table_ready table=%s column_count=%d",
        _data_table_name(model_id),
        len(columns),
    )


def insert_rows(model_id: str, rows: Iterable[Dict[str, Any]]) -> None:
    rows_list = list(rows)
    if not rows_list:
        return
    columns = list(rows_list[0].keys())
    values = [[row[column] for column in columns] for row in rows_list]
    client = get_client()
    client.insert(_data_table_name(model_id), values, column_names=columns)
    logger.info(
        "clickhouse_rows_inserted table=%s row_count=%d",
        _data_table_name(model_id),
        len(rows_list),
    )


def ensure_rejected_table() -> None:
    ddl = """
        CREATE TABLE IF NOT EXISTS rejected_rows (
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
        ORDER BY (job_id, row)
    """
    client = get_client()
    client.command(ddl)
    logger.info("clickhouse_table_ready table=rejected_rows column_count=9")


def insert_rejected_rows(job_id: str, rows: Iterable[Dict[str, Any]]) -> None:
    payload = [
        {
            "job_id": job_id,
            "row": int(row["row"]),
            "event_id": row.get("event_id", ""),
            "column": row.get("column", ""),
            "type": row.get("type", ""),
            "error": row.get("error", ""),
            "observed": row.get("observed", ""),
            "message": row.get("message", ""),
        }
        for row in rows
    ]
    if not payload:
        return
    columns = ["job_id", "row", "event_id", "column", "type", "error", "observed", "message"]
    values = [[row[column] for column in columns] for row in payload]
    client = get_client()
    client.insert("rejected_rows", values, column_names=columns)
    logger.info("clickhouse_rejected_rows_inserted job_id=%s row_count=%d", job_id, len(payload))
