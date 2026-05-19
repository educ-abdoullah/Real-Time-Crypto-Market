import asyncio
import os
from datetime import datetime, timezone

import orjson
from aiokafka import AIOKafkaConsumer
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import BulkWriteError


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "crypto.trades.raw")

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:root@localhost:27017/?authSource=admin"
)

DB_NAME = os.getenv("MONGO_DB", "crypto")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "trades")

BATCH_SIZE = int(os.getenv("MONGO_BATCH_SIZE", "1000"))
BATCH_TIMEOUT_MS = int(os.getenv("MONGO_BATCH_TIMEOUT_MS", "500"))


def ms_to_datetime(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


async def create_indexes(collection):
    await collection.create_index(
        [("source", 1), ("symbol", 1), ("trade_id", 1)],
        unique=True
    )

    await collection.create_index(
        [("symbol", 1), ("exchange_time", -1)]
    )

    await collection.create_index(
        [("exchange_time", -1)]
    )


def normalize_for_mongo(trade: dict) -> dict:
    exchange_ts = int(trade["exchange_ts"])
    ingest_ts = int(trade["ingest_ts"])

    return {
        "source": trade["source"],
        "symbol": trade["symbol"],
        "price": float(trade["price"]),
        "volume": float(trade["volume"]),
        "notional": float(trade.get("notional", float(trade["price"]) * float(trade["volume"]))),
        "trade_id": str(trade["trade_id"]),

        "exchange_ts": exchange_ts,
        "exchange_time": ms_to_datetime(exchange_ts),

        "ingest_ts": ingest_ts,
        "ingest_time": ms_to_datetime(ingest_ts),

        "is_buyer_market_maker": trade.get("is_buyer_market_maker"),

        "raw": trade
    }


async def main():
    mongo_client = AsyncIOMotorClient(MONGO_URI)

    await mongo_client.admin.command("ping")

    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]

    await create_indexes(collection)

    consumer = AIOKafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="mongo-storage-consumer",
        group_id="storage-mongo-consumer",

        auto_offset_reset="earliest",
        enable_auto_commit=False,

        value_deserializer=lambda value: orjson.loads(value),

        fetch_min_bytes=1,
        fetch_max_wait_ms=500,
        max_poll_records=BATCH_SIZE
    )

    await consumer.start()

    print(f"Consumer MongoDB connecté à Kafka : {KAFKA_BOOTSTRAP}")
    print(f"Topic lu : {TOPIC_RAW}")
    print(f"MongoDB : {DB_NAME}.{COLLECTION_NAME}")
    print(f"Batch size : {BATCH_SIZE}")

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
                        document = normalize_for_mongo(message.value)
                        documents.append(document)
                    except Exception as error:
                        print(f"Message invalide ignoré : {error}")

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
                f"Batch MongoDB | reçus={len(documents)} | "
                f"insérés={inserted_count} | total={total_inserted}"
            )

    except asyncio.CancelledError:
        print("Arrêt demandé du consumer MongoDB...")
        raise

    finally:
        print("Fermeture du consumer MongoDB...")
        await consumer.stop()
        mongo_client.close()
        print("Consumer MongoDB arrêté.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Script arrêté avec CTRL + C.")