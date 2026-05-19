import asyncio
import os
import time
from collections import defaultdict

import orjson
import websockets
from aiokafka import AIOKafkaProducer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "crypto.trades.raw")

SYMBOLS = [
    symbol.strip().lower()
    for symbol in os.getenv("BINANCE_SYMBOLS", "btcusdt,ethusdt").split(",")
    if symbol.strip()
]

STREAMS = "/".join([f"{symbol}@trade" for symbol in SYMBOLS])
BINANCE_WS_URL = f"wss://stream.binance.com:9443/stream?streams={STREAMS}"


def normalize_market_from_binance(symbol: str) -> dict:
    symbol = symbol.upper()

    if symbol.endswith("USDT"):
        base_asset = symbol[:-4]
        quote_asset = "USDT"
        market = f"{base_asset}-USD"
    elif symbol.endswith("USD"):
        base_asset = symbol[:-3]
        quote_asset = "USD"
        market = f"{base_asset}-USD"
    else:
        base_asset = symbol
        quote_asset = "UNKNOWN"
        market = symbol

    return {
        "market": market,
        "base_asset": base_asset,
        "quote_asset": quote_asset
    }


def normalize_binance_trade(data: dict) -> dict:
    price = float(data["p"])
    volume = float(data["q"])
    symbol = data["s"].upper()

    market_info = normalize_market_from_binance(symbol)

    return {
        "source": "binance",
        "market": market_info["market"],
        "symbol": symbol,
        "base_asset": market_info["base_asset"],
        "quote_asset": market_info["quote_asset"],

        "price": price,
        "volume": volume,
        "notional": price * volume,

        "trade_id": str(data["t"]),
        "exchange_ts": int(data["T"]),
        "ingest_ts": int(time.time() * 1000),

        "side": None,
        "is_buyer_market_maker": data.get("m")
    }


async def create_kafka_producer() -> AIOKafkaProducer:
    while True:
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                client_id="binance-websocket-producer",
                value_serializer=lambda value: orjson.dumps(value),
                key_serializer=lambda key: str(key).encode("utf-8"),
                acks=1,
                linger_ms=20,
                compression_type=None,
                request_timeout_ms=30000,
                retry_backoff_ms=500
            )

            await producer.start()

            print(f"Producer Binance connecté à Kafka : {KAFKA_BOOTSTRAP}")
            print(f"Topic destination : {TOPIC_RAW}")
            print("Compression Kafka utilisée : aucune")

            return producer

        except Exception as error:
            print(f"Kafka indisponible, nouvelle tentative dans 5 secondes : {error}")
            await asyncio.sleep(5)


async def stream_binance_to_kafka():
    producer = await create_kafka_producer()

    counters = defaultdict(int)
    last_log_ts = time.time()

    try:
        while True:
            try:
                print(f"Connexion Binance WebSocket : {BINANCE_WS_URL}")

                async with websockets.connect(
                    BINANCE_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=4096,
                    close_timeout=5
                ) as websocket:

                    async for message in websocket:
                        payload = orjson.loads(message)
                        data = payload.get("data", payload)

                        if data.get("e") != "trade":
                            continue

                        event = normalize_binance_trade(data)

                        await producer.send(
                            TOPIC_RAW,
                            key=f"{event['source']}:{event['market']}",
                            value=event
                        )

                        counters[event["market"]] += 1

                        now = time.time()

                        if now - last_log_ts >= 5:
                            total = sum(counters.values())
                            stats = " | ".join(
                                [f"{market}={count}" for market, count in counters.items()]
                            )

                            print(f"Flux Binance actif | total={total} trades | {stats}")

                            last_log_ts = now

            except Exception as error:
                print(f"Erreur Binance WebSocket, reconnexion dans 5 secondes : {error}")
                await asyncio.sleep(5)

    finally:
        await producer.flush()
        await producer.stop()
        print("Producer Binance arrêté proprement.")


if __name__ == "__main__":
    try:
        asyncio.run(stream_binance_to_kafka())
    except KeyboardInterrupt:
        print("Script Binance arrêté avec CTRL + C.")