import asyncio
import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

METRICS_TOPIC = "crypto.metrics.rolling"
ALERTS_TOPIC = "crypto.alerts"

# Seuils d'anomalies
PRICE_CHANGE_THRESHOLD_PERCENT = float(os.getenv("PRICE_CHANGE_THRESHOLD_PERCENT", "0.20"))
VOLATILITY_THRESHOLD_PERCENT = float(os.getenv("VOLATILITY_THRESHOLD_PERCENT", "0.30"))
VOLUME_SPIKE_RATIO = float(os.getenv("VOLUME_SPIKE_RATIO", "3.0"))

# Pour éviter de spammer la même alerte toutes les 5 secondes
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "30"))

# Historique des volumes pour calculer une moyenne
volume_history = defaultdict(lambda: deque(maxlen=20))

# Dernière fois où une alerte a été envoyée
last_alert_time = {}


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def can_send_alert(symbol: str, alert_type: str, window: str) -> bool:
    key = f"{symbol}:{alert_type}:{window}"
    current_time = time.time()

    last_time = last_alert_time.get(key, 0)

    if current_time - last_time >= ALERT_COOLDOWN_SECONDS:
        last_alert_time[key] = current_time
        return True

    return False


def build_alert(
    symbol: str,
    window: str,
    alert_type: str,
    severity: str,
    message: str,
    metric: dict
) -> dict:
    return {
        "type": "anomaly_alert",
        "symbol": symbol,
        "window": window,
        "alert_type": alert_type,
        "severity": severity,
        "message": message,
        "created_ts": now_ms(),
        "created_time": utc_now_iso(),
        "metric": metric,
    }


def detect_price_change(metric: dict):
    symbol = metric["symbol"]
    window = metric["window"]
    variation = float(metric["price_change_percent"])

    if abs(variation) >= PRICE_CHANGE_THRESHOLD_PERCENT:
        severity = "HIGH" if abs(variation) >= PRICE_CHANGE_THRESHOLD_PERCENT * 2 else "MEDIUM"

        direction = "hausse" if variation > 0 else "baisse"

        return build_alert(
            symbol=symbol,
            window=window,
            alert_type="PRICE_CHANGE",
            severity=severity,
            message=(
                f"Variation importante du prix sur {window} : "
                f"{direction} de {variation:.4f}%"
            ),
            metric=metric,
        )

    return None


def detect_volatility(metric: dict):
    symbol = metric["symbol"]
    window = metric["window"]

    avg_price = float(metric["avg_price"])
    min_price = float(metric["min_price"])
    max_price = float(metric["max_price"])

    if avg_price == 0:
        return None

    volatility_percent = ((max_price - min_price) / avg_price) * 100

    if volatility_percent >= VOLATILITY_THRESHOLD_PERCENT:
        severity = "HIGH" if volatility_percent >= VOLATILITY_THRESHOLD_PERCENT * 2 else "MEDIUM"

        return build_alert(
            symbol=symbol,
            window=window,
            alert_type="HIGH_VOLATILITY",
            severity=severity,
            message=(
                f"Forte volatilité sur {window} : "
                f"écart min/max de {volatility_percent:.4f}%"
            ),
            metric={
                **metric,
                "volatility_percent": volatility_percent,
            },
        )

    return None


def detect_volume_spike(metric: dict):
    symbol = metric["symbol"]
    window = metric["window"]
    total_volume = float(metric["total_volume"])

    key = f"{symbol}:{window}"
    history = volume_history[key]

    # On attend d'avoir un peu d'historique avant de comparer
    if len(history) < 5:
        history.append(total_volume)
        return None

    avg_volume = sum(history) / len(history)

    history.append(total_volume)

    if avg_volume == 0:
        return None

    ratio = total_volume / avg_volume

    if ratio >= VOLUME_SPIKE_RATIO:
        severity = "HIGH" if ratio >= VOLUME_SPIKE_RATIO * 2 else "MEDIUM"

        return build_alert(
            symbol=symbol,
            window=window,
            alert_type="VOLUME_SPIKE",
            severity=severity,
            message=(
                f"Pic de volume sur {window} : "
                f"volume actuel {ratio:.2f} fois supérieur à la moyenne récente"
            ),
            metric={
                **metric,
                "avg_recent_volume": avg_volume,
                "volume_spike_ratio": ratio,
            },
        )

    return None


def detect_anomalies(metric: dict) -> list:
    alerts = []

    detectors = [
        detect_price_change,
        detect_volatility,
        detect_volume_spike,
    ]

    for detector in detectors:
        alert = detector(metric)

        if alert is None:
            continue

        symbol = alert["symbol"]
        alert_type = alert["alert_type"]
        window = alert["window"]

        if can_send_alert(symbol, alert_type, window):
            alerts.append(alert)

    return alerts


async def main():
    consumer = AIOKafkaConsumer(
        METRICS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="alerts-consumer",
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

    print(f"Consumer alertes connecté à Kafka : {KAFKA_BOOTSTRAP}")
    print(f"Lecture depuis : {METRICS_TOPIC}")
    print(f"Écriture vers : {ALERTS_TOPIC}")
    print(f"Seuil variation prix : {PRICE_CHANGE_THRESHOLD_PERCENT}%")
    print(f"Seuil volatilité : {VOLATILITY_THRESHOLD_PERCENT}%")
    print(f"Ratio pic volume : x{VOLUME_SPIKE_RATIO}")

    try:
        async for message in consumer:
            metric = message.value

            symbol = metric["symbol"]
            window = metric["window"]

            alerts = detect_anomalies(metric)

            if not alerts:
                print(
                    f"Pas d'anomalie : {symbol} | "
                    f"window={window} | "
                    f"variation={metric['price_change_percent']:.4f}%"
                )
                continue

            for alert in alerts:
                await producer.send_and_wait(
                    ALERTS_TOPIC,
                    key=f"{alert['symbol']}:{alert['alert_type']}",
                    value=alert,
                )

                print(
                    f"ALERTE publiée : {alert['symbol']} | "
                    f"{alert['alert_type']} | "
                    f"{alert['severity']} | "
                    f"{alert['message']}"
                )

    except asyncio.CancelledError:
        print("Arrêt demandé du consumer alertes...")
        raise

    finally:
        print("Fermeture propre du consumer alertes...")
        await consumer.stop()
        await producer.stop()
        print("Consumer alertes arrêté.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Script arrêté avec CTRL + C.")