import asyncio
import os
from datetime import datetime, timezone

import orjson
from aiokafka import AIOKafkaConsumer
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import BulkWriteError


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_METRICS = os.getenv("KAFKA_TOPIC_METRICS", "crypto.metrics")

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:root@localhost:27017/?authSource=admin"
)

DB_NAME = os.getenv("MONGO_DB", "crypto")
COLLECTION_NAME = os.getenv("MONGO_METRICS_COLLECTION", "metrics")

BATCH_SIZE = int(os.getenv("METRICS_MONGO_BATCH_SIZE", "500"))
BATCH_TIMEOUT_MS = int(os.getenv("METRICS_MONGO_BATCH_TIMEOUT_MS", "1000"))


def ms_to_datetime(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


async def create_indexes(collection):
    await collection.create_index([("source", 1), ("market", 1), ("computed_time", -1)])
    await collection.create_index([("computed_time", -1)])
    await collection.create_index("computed_time", expireAfterSeconds=86400)


def normalize_metric(metric: dict) -> dict:
    computed_ts = int(metric["computed_ts"])

    return {
        "type": metric.get("type", "market_metrics"),
        "source": metric["source"],
        "market": metric["market"],
        "symbol": metric.get("symbol"),

        "computed_ts": computed_ts,
        "computed_time": ms_to_datetime(computed_ts),

        "windows": metric.get("windows", {}),
        "raw": metric
    }


async def main():
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    await mongo_client.admin.command("ping")

    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]

    await create_indexes(collection)

    consumer = AIOKafkaConsumer(
        TOPIC_METRICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="metrics-to-mongo-consumer",
        group_id="metrics-to-mongo-consumer-group",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda value: orjson.loads(value),
        fetch_min_bytes=1,
        fetch_max_wait_ms=BATCH_TIMEOUT_MS,
        max_poll_records=BATCH_SIZE
    )

    await consumer.start()

    print(f"Consumer metrics_to_mongo connecté à Kafka : {KAFKA_BOOTSTRAP}")
    print(f"Topic lu : {TOPIC_METRICS}")
    print(f"MongoDB : {DB_NAME}.{COLLECTION_NAME}")

    total_inserted = 0

    try:
        while True:
            batches = await consumer.getmany(
                timeout_ms=BATCH_TIMEOUT_MS,
                max_records=BATCH_SIZE
            )

            documents = []

            for _, messages in batches.items():
                for message in messages:
                    try:
                        documents.append(normalize_metric(message.value))
                    except Exception as error:
                        print(f"Métrique invalide ignorée : {error}")

            if not documents:
                continue

            inserted_count = 0

            try:
                result = await collection.insert_many(documents, ordered=False)
                inserted_count = len(result.inserted_ids)

            except BulkWriteError as error:
                details = error.details or {}
                inserted_count = details.get("nInserted", 0)

            await consumer.commit()

            total_inserted += inserted_count

            print(
                f"Batch metrics MongoDB | reçus={len(documents)} | "
                f"insérés={inserted_count} | total={total_inserted}"
            )

    except asyncio.CancelledError:
        print("Arrêt demandé du consumer metrics_to_mongo...")
        raise

    finally:
        await consumer.stop()
        mongo_client.close()
        print("Consumer metrics_to_mongo arrêté.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Script arrêté avec CTRL + C.")