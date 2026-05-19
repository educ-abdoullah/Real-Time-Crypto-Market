import asyncio
import os
from datetime import datetime, timezone

import orjson
from aiokafka import AIOKafkaConsumer
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import BulkWriteError


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_ALERTS = os.getenv("KAFKA_TOPIC_ALERTS", "crypto.alerts")

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:root@localhost:27017/?authSource=admin"
)

DB_NAME = os.getenv("MONGO_DB", "crypto")
COLLECTION_NAME = os.getenv("MONGO_ALERTS_COLLECTION", "alerts")

BATCH_SIZE = int(os.getenv("ALERTS_MONGO_BATCH_SIZE", "500"))
BATCH_TIMEOUT_MS = int(os.getenv("ALERTS_MONGO_BATCH_TIMEOUT_MS", "1000"))


def ms_to_datetime(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


async def create_indexes(collection):
    await collection.create_index(
        [("source", 1), ("market", 1), ("created_time", -1)]
    )

    await collection.create_index(
        [("type", 1), ("severity", 1), ("created_time", -1)]
    )

    await collection.create_index(
        [("created_time", -1)]
    )

    await collection.create_index(
        [("source", 1), ("market", 1), ("type", 1), ("trade_id", 1), ("exchange_ts", 1)],
        unique=True
    )


def normalize_alert(alert: dict) -> dict:
    created_ts = int(alert["created_ts"])
    exchange_ts = int(alert.get("exchange_ts", created_ts))

    return {
        "type": alert["type"],
        "severity": alert.get("severity", "LOW"),

        "source": alert["source"],
        "market": alert["market"],
        "symbol": alert.get("symbol"),

        "price": float(alert.get("price", 0)),
        "volume": float(alert.get("volume", 0)),
        "notional": float(alert.get("notional", 0)),

        "trade_id": str(alert.get("trade_id", "")),

        "exchange_ts": exchange_ts,
        "exchange_time": ms_to_datetime(exchange_ts),

        "created_ts": created_ts,
        "created_time": ms_to_datetime(created_ts),

        "message": alert.get("message", ""),
        "details": alert.get("details", {}),

        "raw": alert
    }


async def main():
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    await mongo_client.admin.command("ping")

    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]

    await create_indexes(collection)

    consumer = AIOKafkaConsumer(
        TOPIC_ALERTS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="alerts-to-mongo-consumer",
        group_id="alerts-to-mongo-consumer-group",

        auto_offset_reset="latest",
        enable_auto_commit=False,

        value_deserializer=lambda value: orjson.loads(value),

        fetch_min_bytes=1,
        fetch_max_wait_ms=BATCH_TIMEOUT_MS,
        max_poll_records=BATCH_SIZE
    )

    await consumer.start()

    print(f"Consumer alerts_to_mongo connecté à Kafka : {KAFKA_BOOTSTRAP}")
    print(f"Topic lu : {TOPIC_ALERTS}")
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
                        documents.append(normalize_alert(message.value))
                    except Exception as error:
                        print(f"Alerte invalide ignorée : {error}")

            if not documents:
                continue

            inserted_count = 0

            try:
                result = await collection.insert_many(
                    documents,
                    ordered=False
                )
                inserted_count = len(result.inserted_ids)

            except BulkWriteError as error:
                details = error.details or {}
                inserted_count = details.get("nInserted", 0)

            await consumer.commit()

            total_inserted += inserted_count

            print(
                f"Batch alerts MongoDB | reçus={len(documents)} | "
                f"insérés={inserted_count} | total={total_inserted}"
            )

    except asyncio.CancelledError:
        print("Arrêt demandé du consumer alerts_to_mongo...")
        raise

    finally:
        await consumer.stop()
        mongo_client.close()
        print("Consumer alerts_to_mongo arrêté.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Script alerts_to_mongo arrêté avec CTRL + C.")