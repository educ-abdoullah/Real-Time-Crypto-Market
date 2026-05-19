import asyncio
import os
import time
from collections import defaultdict, deque
from itertools import combinations

import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "crypto.trades.raw")
TOPIC_ALERTS = os.getenv("KAFKA_TOPIC_ALERTS", "crypto.alerts")

WINDOW_SECONDS = int(os.getenv("ALERT_WINDOW_SECONDS", "60"))
MAX_WINDOW_SECONDS = int(os.getenv("ALERT_MAX_WINDOW_SECONDS", "180"))

LARGE_TRADE_NOTIONAL = float(os.getenv("ALERT_LARGE_TRADE_NOTIONAL", "50000"))
PRICE_SPIKE_PCT_60S = float(os.getenv("ALERT_PRICE_SPIKE_PCT_60S", "0.30"))
HIGH_ACTIVITY_TRADES_60S = int(os.getenv("ALERT_HIGH_ACTIVITY_TRADES_60S", "300"))
HIGH_LATENCY_MS = int(os.getenv("ALERT_HIGH_LATENCY_MS", "2000"))

EXCHANGE_DIVERGENCE_PCT = float(os.getenv("ALERT_EXCHANGE_DIVERGENCE_PCT", "0.15"))
EXCHANGE_PRICE_MAX_AGE_MS = int(os.getenv("ALERT_EXCHANGE_PRICE_MAX_AGE_MS", "5000"))

ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "30"))

BATCH_TIMEOUT_MS = int(os.getenv("ALERT_BATCH_TIMEOUT_MS", "500"))
MAX_RECORDS = int(os.getenv("ALERT_MAX_RECORDS", "3000"))


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
    symbol = trade.get("symbol", trade.get("market", "UNKNOWN"))

    return {
        "source": trade.get("source", "unknown"),
        "market": trade.get("market", infer_market(symbol)),
        "symbol": symbol,

        "price": price,
        "volume": volume,
        "notional": float(trade.get("notional", price * volume)),

        "trade_id": str(trade["trade_id"]),
        "exchange_ts": int(trade["exchange_ts"]),
        "ingest_ts": int(trade["ingest_ts"]),

        "side": trade.get("side"),
        "is_buyer_market_maker": trade.get("is_buyer_market_maker")
    }


def prune_old_trades(trades_window: deque, cutoff_ts: int) -> None:
    while trades_window and trades_window[0]["exchange_ts"] < cutoff_ts:
        trades_window.popleft()


def can_emit_alert(last_alerts: dict, alert_type: str, key: str) -> bool:
    cooldown_key = f"{alert_type}:{key}"
    current_time = time.time()

    last_time = last_alerts.get(cooldown_key)

    if last_time is not None and current_time - last_time < ALERT_COOLDOWN_SECONDS:
        return False

    last_alerts[cooldown_key] = current_time
    return True


def build_alert(
    alert_type: str,
    severity: str,
    trade: dict,
    message: str,
    details: dict
) -> dict:
    created_ts = now_ms()

    return {
        "type": alert_type,
        "severity": severity,

        "source": trade["source"],
        "market": trade["market"],
        "symbol": trade["symbol"],

        "price": trade["price"],
        "volume": trade["volume"],
        "notional": trade["notional"],

        "trade_id": trade["trade_id"],
        "exchange_ts": trade["exchange_ts"],

        "created_ts": created_ts,
        "message": message,
        "details": details
    }


def build_cross_exchange_alert(
    market: str,
    source_a: str,
    price_a: float,
    ts_a: int,
    source_b: str,
    price_b: float,
    ts_b: int,
    divergence_pct: float
) -> dict:
    created_ts = now_ms()

    return {
        "type": "EXCHANGE_PRICE_DIVERGENCE",
        "severity": "HIGH",

        "source": "multi_exchange",
        "market": market,
        "symbol": market,

        "price": max(price_a, price_b),
        "volume": 0.0,
        "notional": 0.0,

        "trade_id": f"{source_a}-{source_b}-{market}-{created_ts}",
        "exchange_ts": max(ts_a, ts_b),

        "created_ts": created_ts,

        "message": (
            f"Écart de prix détecté entre {source_a} et {source_b} "
            f"sur {market} : {round(divergence_pct, 4)}%"
        ),

        "details": {
            "platform_a": source_a,
            "platform_b": source_b,
            "price_a": price_a,
            "price_b": price_b,
            "timestamp_a": ts_a,
            "timestamp_b": ts_b,
            "divergence_pct": divergence_pct,
            "threshold_pct": EXCHANGE_DIVERGENCE_PCT
        }
    }


def compute_price_change_pct(trades: list[dict]) -> float:
    if len(trades) < 2:
        return 0.0

    first_price = trades[0]["price"]
    last_price = trades[-1]["price"]

    if first_price <= 0:
        return 0.0

    return ((last_price - first_price) / first_price) * 100


async def send_alert(producer: AIOKafkaProducer, alert: dict):
    key = f"{alert['source']}:{alert['market']}:{alert['type']}"

    await producer.send(
        TOPIC_ALERTS,
        key=key,
        value=alert
    )

    print(
        f"ALERTE | {alert['type']} | {alert['severity']} | "
        f"{alert['source']} | {alert['market']} | {alert['message']}"
    )


def check_exchange_divergence(latest_prices_by_market: dict, last_alerts: dict) -> list[dict]:
    alerts = []
    current_ts = now_ms()

    for market, prices_by_source in latest_prices_by_market.items():
        active_sources = {}

        for source, data in prices_by_source.items():
            age_ms = current_ts - data["ts"]

            if age_ms <= EXCHANGE_PRICE_MAX_AGE_MS:
                active_sources[source] = data

        if len(active_sources) < 2:
            continue

        for source_a, source_b in combinations(active_sources.keys(), 2):
            data_a = active_sources[source_a]
            data_b = active_sources[source_b]

            price_a = data_a["price"]
            price_b = data_b["price"]

            if price_a <= 0 or price_b <= 0:
                continue

            reference_price = min(price_a, price_b)
            divergence_pct = (abs(price_a - price_b) / reference_price) * 100

            alert_key = f"{market}:{source_a}:{source_b}"

            if (
                divergence_pct >= EXCHANGE_DIVERGENCE_PCT
                and can_emit_alert(last_alerts, "EXCHANGE_PRICE_DIVERGENCE", alert_key)
            ):
                alert = build_cross_exchange_alert(
                    market=market,
                    source_a=source_a,
                    price_a=price_a,
                    ts_a=data_a["ts"],
                    source_b=source_b,
                    price_b=price_b,
                    ts_b=data_b["ts"],
                    divergence_pct=divergence_pct
                )

                alerts.append(alert)

    return alerts


async def create_consumer() -> AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="alerts-consumer",
        group_id="alerts-consumer-group",
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
        client_id="alerts-producer",
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
    latest_prices_by_market = defaultdict(dict)
    last_alerts = {}

    total_received = 0

    print(f"Consumer alertes connecté à Kafka : {KAFKA_BOOTSTRAP}")
    print(f"Topic lu : {TOPIC_RAW}")
    print(f"Topic écrit : {TOPIC_ALERTS}")
    print("Alertes actives : LARGE_TRADE, PRICE_SPIKE, HIGH_ACTIVITY, HIGH_LATENCY, EXCHANGE_PRICE_DIVERGENCE")

    try:
        while True:
            batches = await consumer.getmany(
                timeout_ms=BATCH_TIMEOUT_MS,
                max_records=MAX_RECORDS
            )

            current_ts = now_ms()
            max_cutoff_ts = current_ts - (MAX_WINDOW_SECONDS * 1000)

            for _, messages in batches.items():
                for message in messages:
                    try:
                        trade = normalize_trade(message.value)
                        key = f"{trade['source']}:{trade['market']}"

                        trades_by_key[key].append(trade)

                        latest_prices_by_market[trade["market"]][trade["source"]] = {
                            "price": trade["price"],
                            "ts": trade["exchange_ts"],
                            "ingest_ts": trade["ingest_ts"],
                            "symbol": trade["symbol"]
                        }

                        total_received += 1

                        latency_ms = trade["ingest_ts"] - trade["exchange_ts"]

                        if (
                            latency_ms > HIGH_LATENCY_MS
                            and can_emit_alert(last_alerts, "HIGH_LATENCY", key)
                        ):
                            alert = build_alert(
                                alert_type="HIGH_LATENCY",
                                severity="MEDIUM",
                                trade=trade,
                                message=(
                                    f"Latence élevée détectée sur {trade['source']} "
                                    f"{trade['market']} : {latency_ms} ms"
                                ),
                                details={
                                    "platform": trade["source"],
                                    "latency_ms": latency_ms,
                                    "threshold_ms": HIGH_LATENCY_MS
                                }
                            )
                            await send_alert(producer, alert)

                        if (
                            trade["notional"] >= LARGE_TRADE_NOTIONAL
                            and can_emit_alert(last_alerts, "LARGE_TRADE", key)
                        ):
                            alert = build_alert(
                                alert_type="LARGE_TRADE",
                                severity="HIGH",
                                trade=trade,
                                message=(
                                    f"Grosse transaction détectée sur {trade['source']} "
                                    f"{trade['market']} : {round(trade['notional'], 2)} USD"
                                ),
                                details={
                                    "platform": trade["source"],
                                    "notional": trade["notional"],
                                    "threshold": LARGE_TRADE_NOTIONAL
                                }
                            )
                            await send_alert(producer, alert)

                    except Exception as error:
                        print(f"Trade invalide ignoré : {error}")

            for key, trades_window in trades_by_key.items():
                prune_old_trades(trades_window, max_cutoff_ts)

                current_cutoff_ts = current_ts - (WINDOW_SECONDS * 1000)

                trades_60s = [
                    trade
                    for trade in trades_window
                    if trade["exchange_ts"] >= current_cutoff_ts
                ]

                if not trades_60s:
                    continue

                last_trade = trades_60s[-1]

                price_change_pct = compute_price_change_pct(trades_60s)

                if (
                    abs(price_change_pct) >= PRICE_SPIKE_PCT_60S
                    and can_emit_alert(last_alerts, "PRICE_SPIKE", key)
                ):
                    alert = build_alert(
                        alert_type="PRICE_SPIKE",
                        severity="HIGH",
                        trade=last_trade,
                        message=(
                            f"Variation brutale du prix sur {last_trade['source']} "
                            f"{last_trade['market']} sur 60s : {round(price_change_pct, 4)}%"
                        ),
                        details={
                            "platform": last_trade["source"],
                            "price_change_pct": price_change_pct,
                            "threshold_pct": PRICE_SPIKE_PCT_60S,
                            "window_seconds": WINDOW_SECONDS
                        }
                    )
                    await send_alert(producer, alert)

                if (
                    len(trades_60s) >= HIGH_ACTIVITY_TRADES_60S
                    and can_emit_alert(last_alerts, "HIGH_ACTIVITY", key)
                ):
                    alert = build_alert(
                        alert_type="HIGH_ACTIVITY",
                        severity="MEDIUM",
                        trade=last_trade,
                        message=(
                            f"Activité élevée sur {last_trade['source']} "
                            f"{last_trade['market']} : {len(trades_60s)} trades sur 60s"
                        ),
                        details={
                            "platform": last_trade["source"],
                            "trade_count_60s": len(trades_60s),
                            "threshold": HIGH_ACTIVITY_TRADES_60S,
                            "window_seconds": WINDOW_SECONDS
                        }
                    )
                    await send_alert(producer, alert)

            divergence_alerts = check_exchange_divergence(
                latest_prices_by_market=latest_prices_by_market,
                last_alerts=last_alerts
            )

            for alert in divergence_alerts:
                await send_alert(producer, alert)

            await producer.flush()
            await consumer.commit()

    except asyncio.CancelledError:
        print("Arrêt demandé du consumer alertes...")
        raise

    finally:
        await consumer.stop()
        await producer.stop()
        print("Consumer alertes arrêté.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Script alertes arrêté avec CTRL + C.")