import asyncio
import math
import os
import time
from collections import defaultdict, deque

import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "crypto.trades.raw")
TOPIC_METRICS = os.getenv("KAFKA_TOPIC_METRICS", "crypto.metrics")

WINDOWS_SECONDS = [
    int(value.strip())
    for value in os.getenv("METRICS_WINDOWS_SECONDS", "60,180,240,300,900").split(",")
    if value.strip()
]

EMIT_INTERVAL_SECONDS = float(os.getenv("METRICS_EMIT_INTERVAL_SECONDS", "1"))
BATCH_TIMEOUT_MS = int(os.getenv("METRICS_BATCH_TIMEOUT_MS", "500"))
MAX_RECORDS = int(os.getenv("METRICS_MAX_RECORDS", "3000"))

MAX_WINDOW_SECONDS = max(WINDOWS_SECONDS)


def now_ms() -> int:
    return int(time.time() * 1000)


def infer_market(symbol: str) -> str:
    symbol = symbol.upper()

    if "-" in symbol:
        base, _ = symbol.split("-", 1)
        return f"{base}-USD"

    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}-USD"

    if symbol.endswith("USD"):
        return f"{symbol[:-3]}-USD"

    return symbol


def normalize_trade(trade: dict) -> dict:
    price = float(trade["price"])
    volume = float(trade["volume"])
    notional = float(trade.get("notional", price * volume))

    source = trade.get("source", "unknown")
    symbol = trade.get("symbol", trade.get("market", "UNKNOWN"))
    market = trade.get("market", infer_market(symbol))

    return {
        "source": source,
        "market": market,
        "symbol": symbol,

        "base_asset": trade.get("base_asset"),
        "quote_asset": trade.get("quote_asset"),

        "price": price,
        "volume": volume,
        "notional": notional,

        "trade_id": str(trade["trade_id"]),
        "exchange_ts": int(trade["exchange_ts"]),
        "ingest_ts": int(trade["ingest_ts"]),

        "side": trade.get("side"),
        "is_buyer_market_maker": trade.get("is_buyer_market_maker")
    }


def prune_old_trades(trades_window: deque, cutoff_ts: int) -> None:
    while trades_window and trades_window[0]["exchange_ts"] < cutoff_ts:
        trades_window.popleft()


def compute_std(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0

    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)

    return math.sqrt(variance)


def compute_window_metrics(trades: list[dict], window_seconds: int) -> dict | None:
    if not trades:
        return None

    first_trade = trades[0]
    last_trade = trades[-1]

    prices = [trade["price"] for trade in trades]
    volumes = [trade["volume"] for trade in trades]
    notionals = [trade["notional"] for trade in trades]

    trade_count = len(trades)
    volume_sum = sum(volumes)
    notional_sum = sum(notionals)

    first_price = first_trade["price"]
    last_price = last_trade["price"]

    avg_price = sum(prices) / trade_count
    vwap = notional_sum / volume_sum if volume_sum > 0 else last_price

    price_change = last_price - first_price
    price_change_pct = (price_change / first_price) * 100 if first_price > 0 else 0.0

    buy_volume = 0.0
    sell_volume = 0.0

    for trade in trades:
        if trade.get("source") == "binance":
            if trade.get("is_buyer_market_maker") is True:
                sell_volume += trade["volume"]
            elif trade.get("is_buyer_market_maker") is False:
                buy_volume += trade["volume"]

        elif trade.get("source") == "coinbase":
            if trade.get("side") == "buy":
                buy_volume += trade["volume"]
            elif trade.get("side") == "sell":
                sell_volume += trade["volume"]

    buy_volume_ratio = (buy_volume / volume_sum) * 100 if volume_sum > 0 else 0.0
    sell_volume_ratio = (sell_volume / volume_sum) * 100 if volume_sum > 0 else 0.0

    latencies = [
        trade["ingest_ts"] - trade["exchange_ts"]
        for trade in trades
        if trade.get("ingest_ts") and trade.get("exchange_ts")
    ]

    avg_latency_ms = sum(latencies) / len(latencies) if latencies else 0.0
    max_trade = max(trades, key=lambda trade: trade["notional"])

    return {
        "window_seconds": window_seconds,

        "last_price": round(last_price, 8),
        "avg_price": round(avg_price, 8),
        "vwap": round(vwap, 8),

        "volume": round(volume_sum, 8),
        "notional": round(notional_sum, 8),

        "price_change": round(price_change, 8),
        "price_change_pct": round(price_change_pct, 4),

        "high_price": round(max(prices), 8),
        "low_price": round(min(prices), 8),
        "price_volatility": round(compute_std(prices), 8),

        "trade_count": trade_count,
        "trades_per_second": round(trade_count / window_seconds, 4),

        "buy_volume": round(buy_volume, 8),
        "sell_volume": round(sell_volume, 8),
        "buy_volume_ratio": round(buy_volume_ratio, 2),
        "sell_volume_ratio": round(sell_volume_ratio, 2),

        "max_trade_volume": round(max_trade["volume"], 8),
        "max_trade_notional": round(max_trade["notional"], 8),

        "avg_latency_ms": round(avg_latency_ms, 2),

        "first_exchange_ts": first_trade["exchange_ts"],
        "last_exchange_ts": last_trade["exchange_ts"]
    }


def compute_all_metrics(source: str, market: str, symbol: str, trades_window: deque) -> dict | None:
    if not trades_window:
        return None

    current_ts = now_ms()
    windows = {}

    for window_seconds in WINDOWS_SECONDS:
        cutoff_ts = current_ts - (window_seconds * 1000)

        trades_for_window = [
            trade
            for trade in trades_window
            if trade["exchange_ts"] >= cutoff_ts
        ]

        metrics = compute_window_metrics(
            trades=trades_for_window,
            window_seconds=window_seconds
        )

        if metrics:
            windows[f"{window_seconds}s"] = metrics

    if not windows:
        return None

    return {
        "type": "market_metrics",
        "source": source,
        "market": market,
        "symbol": symbol,
        "computed_ts": current_ts,
        "windows": windows
    }


async def create_consumer() -> AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="metrics-consumer",
        group_id="metrics-consumer-group",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda value: orjson.loads(value),
        fetch_min_bytes=1,
        fetch_max_wait_ms=BATCH_TIMEOUT_MS,
        max_poll_records=MAX_RECORDS
    )

    await consumer.start()
    return consumer


async def create_producer() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="metrics-producer",
        value_serializer=lambda value: orjson.dumps(value),
        key_serializer=lambda key: str(key).encode("utf-8"),
        acks=1,
        linger_ms=20,
        compression_type=None,
        request_timeout_ms=30000,
        retry_backoff_ms=500
    )

    await producer.start()
    return producer


async def main():
    consumer = await create_consumer()
    producer = await create_producer()

    trades_by_key = defaultdict(deque)

    total_received = 0
    total_metrics_sent = 0
    last_emit_time = time.time()

    print(f"Consumer métriques connecté à Kafka : {KAFKA_BOOTSTRAP}")
    print(f"Topic lu : {TOPIC_RAW}")
    print(f"Topic écrit : {TOPIC_METRICS}")
    print(f"Fenêtres calculées : {WINDOWS_SECONDS}")

    try:
        while True:
            batches = await consumer.getmany(
                timeout_ms=BATCH_TIMEOUT_MS,
                max_records=MAX_RECORDS
            )

            for _, messages in batches.items():
                for message in messages:
                    try:
                        trade = normalize_trade(message.value)

                        key = f"{trade['source']}:{trade['market']}"
                        trades_by_key[key].append(trade)

                        total_received += 1

                    except Exception as error:
                        print(f"Trade invalide ignoré : {error}")

            current_ts = now_ms()
            max_cutoff_ts = current_ts - (MAX_WINDOW_SECONDS * 1000)

            for trades_window in trades_by_key.values():
                prune_old_trades(trades_window, max_cutoff_ts)

            current_time = time.time()

            if current_time - last_emit_time >= EMIT_INTERVAL_SECONDS:
                metrics_sent_now = 0

                for key, trades_window in trades_by_key.items():
                    if not trades_window:
                        continue

                    last_trade = trades_window[-1]

                    metrics = compute_all_metrics(
                        source=last_trade["source"],
                        market=last_trade["market"],
                        symbol=last_trade["symbol"],
                        trades_window=trades_window
                    )

                    if not metrics:
                        continue

                    await producer.send(
                        TOPIC_METRICS,
                        key=key,
                        value=metrics
                    )

                    metrics_sent_now += 1
                    total_metrics_sent += 1

                await producer.flush()
                await consumer.commit()

                if metrics_sent_now > 0:
                    print(
                        f"Métriques envoyées | "
                        f"séries={metrics_sent_now} | "
                        f"trades reçus={total_received} | "
                        f"metrics total={total_metrics_sent}"
                    )

                last_emit_time = current_time

    except asyncio.CancelledError:
        print("Arrêt demandé du consumer métriques...")
        raise

    finally:
        await consumer.stop()
        await producer.stop()
        print("Consumer métriques arrêté.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Script arrêté avec CTRL + C.")