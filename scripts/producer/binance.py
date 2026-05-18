import asyncio
import json
import os
import time

import websockets
from aiokafka import AIOKafkaProducer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "crypto.trades.raw"

SYMBOLS = os.getenv("SYMBOLS", "btcusdt,ethusdt").split(",")

streams = "/".join([f"{symbol}@trade" for symbol in SYMBOLS])
BINANCE_WS_URL = f"wss://stream.binance.com:9443/stream?streams={streams}"


def normalize_binance_trade(data: dict) -> dict:
    return {
        "source": "binance",
        "symbol": data["s"],
        "price": float(data["p"]),
        "volume": float(data["q"]),
        "trade_id": str(data["t"]),
        "exchange_ts": int(data["T"]),
        "ingest_ts": int(time.time() * 1000),
    }


async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )

    await producer.start()

    try:
        while True:
            try:
                print(f"Connexion Binance : {BINANCE_WS_URL}")

                async with websockets.connect(BINANCE_WS_URL) as websocket:
                    async for message in websocket:
                        payload = json.loads(message)

                        data = payload.get("data", payload)

                        if data.get("e") != "trade":
                            continue

                        event = normalize_binance_trade(data)

                        await producer.send_and_wait(
                            TOPIC,
                            key=event["symbol"],
                            value=event,
                        )

                        print(event)

            except Exception as error:
                print(f"Erreur WebSocket, reconnexion dans 5 sec : {error}")
                await asyncio.sleep(5)

    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())