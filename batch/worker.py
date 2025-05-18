import asyncio
import csv
import io
import logging
import os
from typing import List, Dict

import pyarrow.parquet as pq
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from . import clickhouse


def _parse_rows(data: bytes) -> List[Dict[str, str]]:
    """Return a list of rows parsed from CSV or Parquet bytes."""
    try:
        table = pq.read_table(io.BytesIO(data))
        return [{k: str(v) for k, v in row.items()} for row in table.to_pylist()]
    except Exception:
        text = data.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in reader]


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
KAFKA_MAX_RETRIES = 12


async def _start(client):
    attempts = 0
    while True:
        try:
            await client.start()
            break
        except Exception as exc:
            attempts += 1
            if attempts >= KAFKA_MAX_RETRIES:
                logger.error(
                    "Kafka connect failed after %s attempts: %s", attempts, exc
                )
                raise
            logger.warning("Kafka connect failed: %s. Retrying...", exc)
            await asyncio.sleep(5)


async def consume(job_id: str, model_id: str) -> None:
    """Consume the job topic and insert rows into ClickHouse."""
    topic = f"/batch/{job_id}"
    dlq_topic = f"/batch/{job_id}/dlq"
    consumer = AIOKafkaConsumer(topic, bootstrap_servers=KAFKA_BOOTSTRAP)
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await _start(consumer)
    await _start(producer)

    try:
        async for msg in consumer:
            try:
                logger.info("Got message from %s offset %s", topic, msg.offset)
                rows = _parse_rows(msg.value)
                if rows:
                    clickhouse.ensure_table(
                        model_id, {c: "String" for c in rows[0].keys()}
                    )
                    clickhouse.insert_rows(model_id, rows)
                    logger.info("Inserted %d rows into data_%s", len(rows), model_id)
            except Exception as exc:
                logger.error("Processing failed: %s", exc)
                await producer.send_and_wait(dlq_topic, msg.value)
    finally:
        await consumer.stop()
        await producer.stop()


def main(job_id: str = "testjob", model_id: str = "model") -> None:
    """Entry point for the worker."""
    asyncio.run(consume(job_id, model_id))


if __name__ == "__main__":
    env_job = os.environ.get("JOB_ID", "testjob")
    env_model = os.environ.get("MODEL_ID", "model")
    main(env_job, env_model)
