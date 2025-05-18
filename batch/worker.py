import asyncio
import logging
import os
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")


async def _start(client):
    while True:
        try:
            await client.start()
            break
        except Exception as exc:
            logger.warning("Kafka connect failed: %s. Retrying...", exc)
            await asyncio.sleep(5)


async def consume(job_id: str):
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
            except Exception as exc:
                logger.error("Processing failed: %s", exc)
                await producer.send_and_wait(dlq_topic, msg.value)
    finally:
        await consumer.stop()
        await producer.stop()


def main(job_id: str = "testjob"):
    asyncio.run(consume(job_id))


if __name__ == "__main__":
    main()
