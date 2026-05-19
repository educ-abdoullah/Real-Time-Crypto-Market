import asyncio
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import orjson
import websockets
from aiokafka import AIOKafkaProducer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "crypto.trades.raw")

COINBASE_WS_URL = os.getenv(
    "COINBASE_WS_URL",
    "wss://advanced-trade-ws.coinbase.com"
)

PRODUCT_IDS = [
    product.strip().upper()
    for product in os.getenv("COINBASE_PRODUCT_IDS", "BTC-USD,ETH-USD").split(",")
    if product.strip()
]


def iso_to_ms(value: str) -> int:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def normalize_coinbase_market_trade(trade: dict) -> dict:
    product_id = trade["product_id"].upper()
    base_asset, quote_asset = product_id.split("-")

    price = float(trade["price"])
    volume = float(trade["size"])

    return {
        "source": "coinbase",
        "market": f"{base_asset}-USD",
        "symbol": product_id,
        "base_asset": base_asset,
        "quote_asset": quote_asset,

        "price": price,
        "volume": volume,
        "notional": price * volume,

        "trade_id": str(trade["trade_id"]),
        "exchange_ts": iso_to_ms(trade["time"]),
        "ingest_ts": int(time.time() * 1000),

        "side": str(trade.get("side", "")).lower(),
        "is_buyer_market_maker": None
    }


async def create_kafka_producer() -> AIOKafkaProducer:
    while True:
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                client_id="coinbase-websocket-producer",

                value_serializer=lambda value: orjson.dumps(value),
                key_serializer=lambda key: str(key).encode("utf-8"),

                acks=1,
                linger_ms=20,
                compression_type=None,
                request_timeout_ms=30000,
                retry_backoff_ms=500
            )

            await producer.start()

            print(f"Producer Coinbase connecté à Kafka : {KAFKA_BOOTSTRAP}")
            print(f"Endpoint Coinbase : {COINBASE_WS_URL}")
            print(f"Topic destination : {TOPIC_RAW}")
            print(f"Produits suivis : {PRODUCT_IDS}")

            return producer

        except Exception as error:
            print(f"Kafka indisponible, nouvelle tentative dans 5 secondes : {error}")
            await asyncio.sleep(5)


async def stream_coinbase_to_kafka():
    producer = await create_kafka_producer()

    counters = defaultdict(int)
    last_log_ts = time.time()

    subscribe_message = {
        "type": "subscribe",
        "product_ids": PRODUCT_IDS,
        "channel": "market_trades"
    }

    try:
        while True:
            try:
                print(f"Connexion Coinbase WebSocket : {COINBASE_WS_URL}")

                async with websockets.connect(
                    COINBASE_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=4096,
                    close_timeout=5
                ) as websocket:

                    await websocket.send(orjson.dumps(subscribe_message).decode("utf-8"))
                    print("Abonnement Coinbase market_trades envoyé.")

                    async for message in websocket:
                        payload = orjson.loads(message)

                        if payload.get("channel") != "market_trades":
                            continue

                        events = payload.get("events", [])

                        for event in events:
                            trades = event.get("trades", [])

                            for trade in trades:
                                normalized_trade = normalize_coinbase_market_trade(trade)

                                await producer.send(
                                    TOPIC_RAW,
                                    key=f"{normalized_trade['source']}:{normalized_trade['market']}",
                                    value=normalized_trade
                                )

                                counters[normalized_trade["market"]] += 1

                        now = time.time()

                        if now - last_log_ts >= 5:
                            total = sum(counters.values())
                            stats = " | ".join(
                                [f"{market}={count}" for market, count in counters.items()]
                            )

                            print(f"Flux Coinbase actif | total={total} trades | {stats}")
                            last_log_ts = now

            except Exception as error:
                print(f"Erreur Coinbase WebSocket, reconnexion dans 5 secondes : {error}")
                await asyncio.sleep(5)

    finally:
        await producer.flush()
        await producer.stop()
        print("Producer Coinbase arrêté proprement.")


if __name__ == "__main__":
    try:
        asyncio.run(stream_coinbase_to_kafka())
    except KeyboardInterrupt:
        print("Script Coinbase arrêté avec CTRL + C.")