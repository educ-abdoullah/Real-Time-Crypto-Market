import asyncio
import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

RAW_TOPIC = "crypto.trades.raw"
METRICS_TOPIC = "crypto.metrics.rolling"

WINDOWS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
}

EMIT_INTERVAL_SECONDS = 5

trades_by_symbol = defaultdict(deque)
last_emit_by_symbol_window = {}


def now_ms() -> int:
    return int(time.time() * 1000)


def clean_old_trades(symbol: str):
    max_window_seconds = max(WINDOWS.values())
    limit_ms = now_ms() - max_window_seconds * 1000

    queue = trades_by_symbol[symbol]

    while queue and queue[0]["ingest_ts"] < limit_ms:
        queue.popleft()


def compute_metrics(symbol: str, window_name: str):
    window_seconds = WINDOWS[window_name]
    limit_ms = now_ms() - window_seconds * 1000

    recent_trades = [
        trade for trade in trades_by_symbol[symbol]
        if trade["ingest_ts"] >= limit_ms
    ]

    if not recent_trades:
        return None

    prices = [trade["price"] for trade in recent_trades]
    volumes = [trade["volume"] for trade in recent_trades]

    first_price = prices[0]
    last_price = prices[-1]

    if first_price != 0:
        price_change_percent = ((last_price - first_price) / first_price) * 100
    else:
        price_change_percent = 0

    return {
        "type": "rolling_metrics",
        "symbol": symbol,
        "window": window_name,

        "avg_price": sum(prices) / len(prices),
        "min_price": min(prices),
        "max_price": max(prices),
        "first_price": first_price,
        "last_price": last_price,
        "price_change_percent": price_change_percent,

        "total_volume": sum(volumes),
        "trade_count": len(recent_trades),

        "computed_ts": now_ms(),
        "computed_time": datetime.now(timezone.utc).isoformat(),
    }


def should_emit(symbol: str, window_name: str) -> bool:
    key = f"{symbol}:{window_name}"
    current_time = time.time()

    last_emit = last_emit_by_symbol_window.get(key, 0)

    if current_time - last_emit >= EMIT_INTERVAL_SECONDS:
        last_emit_by_symbol_window[key] = current_time
        return True

    return False


async def main():
    consumer = AIOKafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="metrics-consumer",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )

    await consumer.start()
    await producer.start()

    print(f"Consumer métriques connecté à Kafka : {KAFKA_BOOTSTRAP}")
    print(f"Lecture depuis : {RAW_TOPIC}")
    print(f"Écriture vers : {METRICS_TOPIC}")

    try:
        async for message in consumer:
            trade = message.value

            symbol = trade["symbol"]

            clean_trade = {
                "symbol": symbol,
                "price": float(trade["price"]),
                "volume": float(trade["volume"]),
                "trade_id": str(trade["trade_id"]),
                "exchange_ts": int(trade["exchange_ts"]),
                "ingest_ts": int(trade["ingest_ts"]),
            }

            trades_by_symbol[symbol].append(clean_trade)
            clean_old_trades(symbol)

            for window_name in WINDOWS.keys():
                if not should_emit(symbol, window_name):
                    continue

                metric = compute_metrics(symbol, window_name)

                if metric is None:
                    continue

                await producer.send_and_wait(
                    METRICS_TOPIC,
                    key=f"{symbol}:{window_name}",
                    value=metric,
                )

                print(
                    f"Métrique publiée : {symbol} | "
                    f"window={window_name} | "
                    f"last={metric['last_price']} | "
                    f"avg={metric['avg_price']:.2f} | "
                    f"variation={metric['price_change_percent']:.4f}% | "
                    f"trades={metric['trade_count']}"
                )

    except asyncio.CancelledError:
        print("Arrêt demandé du consumer métriques...")
        raise

    finally:
        print("Fermeture propre du consumer métriques...")
        await consumer.stop()
        await producer.stop()
        print("Consumer métriques arrêté.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Script arrêté avec CTRL + C.")