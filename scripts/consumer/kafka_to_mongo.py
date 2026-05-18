import asyncio
import json
import os
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "crypto.trades.raw"

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:root@localhost:27017/?authSource=admin"
)

DB_NAME = os.getenv("MONGO_DB", "crypto")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "trades")


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
        "trade_id": str(trade["trade_id"]),

        "exchange_ts": exchange_ts,
        "exchange_time": ms_to_datetime(exchange_ts),

        "ingest_ts": ingest_ts,
        "ingest_time": ms_to_datetime(ingest_ts),

        "is_buyer_market_maker": trade.get("is_buyer_market_maker"),

        "raw": trade,
    }


async def main():
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]

    await create_indexes(collection)

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="storage-mongo-consumer",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    await consumer.start()

    print(f"Consumer connecté à Kafka : {KAFKA_BOOTSTRAP}")
    print(f"Topic lu : {TOPIC}")
    print(f"MongoDB : {MONGO_URI}")
    print(f"Collection : {DB_NAME}.{COLLECTION_NAME}")

    try:
        async for message in consumer:
            trade = message.value
            document = normalize_for_mongo(trade)

            try:
                await collection.insert_one(document)

                print(
                    f"Trade stocké MongoDB : "
                    f"{document['symbol']} | "
                    f"price={document['price']} | "
                    f"volume={document['volume']}"
                )

            except DuplicateKeyError:
                print(
                    f"Trade déjà existant ignoré : "
                    f"{document['symbol']} | trade_id={document['trade_id']}"
                )

            await consumer.commit()

    except asyncio.CancelledError:
        print("Arrêt demandé du consumer...")
        raise

    finally:
        print("Fermeture propre du consumer et de MongoDB...")
        await consumer.stop()
        mongo_client.close()
        print("Consumer MongoDB arrêté.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Script arrêté avec CTRL + C.")