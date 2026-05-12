"""
FinStream Transaction Producer
================================
Generates realistic synthetic credit card transactions and publishes
them to a Kafka topic at configurable throughput.

Intentionally injects anomalous transactions at a configurable rate to
validate the downstream anomaly detection pipeline.
"""

import json
import os
import random
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─── CONFIG ───────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "raw-transactions")
TPS                     = int(os.getenv("TRANSACTIONS_PER_SECOND", 500))
ANOMALY_RATE            = float(os.getenv("ANOMALY_INJECTION_RATE", 0.05))
NUM_USERS               = int(os.getenv("NUM_USERS", 1000))
NUM_MERCHANTS           = int(os.getenv("NUM_MERCHANTS", 500))

fake = Faker()
Faker.seed(42)
random.seed(42)

# ─── REFERENCE DATA ───────────────────────────────────────────────────────────

MERCHANT_CATEGORIES = [
    "GROCERY",
    "ONLINE_RETAIL",
    "RESTAURANT",
    "GAS_STATION",
    "TRAVEL",
    "ENTERTAINMENT",
    "PHARMACY",
    "ELECTRONICS",
    "CLOTHING",
    "CASH_ADVANCE",       # high-risk
    "GAMBLING",           # high-risk
    "CRYPTOCURRENCY",     # high-risk
]

HIGH_RISK_MCC = {"CASH_ADVANCE", "GAMBLING", "CRYPTOCURRENCY"}

COUNTRIES = ["US"] * 70 + ["CA"] * 10 + ["GB"] * 8 + ["DE"] * 5 + ["FR"] * 4 + ["AU"] * 3

# Simulated user spending profiles — each user has a typical spend range
USER_PROFILES: dict[str, dict] = {}

def _init_user_profiles() -> None:
    for i in range(NUM_USERS):
        user_id = f"user_{i:05d}"
        spend_tier = random.choice(["low", "medium", "high"])
        USER_PROFILES[user_id] = {
            "typical_amount_mean": {"low": 35, "medium": 95, "high": 280}[spend_tier],
            "typical_amount_std":  {"low": 20, "medium": 60, "high": 150}[spend_tier],
            "preferred_country":   random.choice(["US", "US", "US", "CA", "GB"]),
            "preferred_categories": random.sample(MERCHANT_CATEGORIES[:9], k=3),
            "active_hours":         list(range(7, 23)),   # typically 7am–11pm
        }

_init_user_profiles()

MERCHANTS: list[dict] = [
    {
        "merchant_id":       f"merch_{i:05d}",
        "merchant_name":     fake.company(),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
    }
    for i in range(NUM_MERCHANTS)
]

# ─── TRANSACTION SCHEMA ───────────────────────────────────────────────────────

@dataclass
class Transaction:
    transaction_id:    str
    card_id:           str
    user_id:           str
    merchant_id:       str
    merchant_name:     str
    merchant_category: str
    amount:            float
    currency:          str
    country:           str
    city:              str
    timestamp:         str
    is_international:  bool
    card_present:      bool
    device_type:       str
    # Injected metadata (consumed by Spark, not surfaced to customers)
    _is_injected_anomaly: bool = False

# ─── TRANSACTION FACTORIES ────────────────────────────────────────────────────

def _normal_transaction(user_id: str, card_id: str) -> Transaction:
    profile  = USER_PROFILES[user_id]
    merchant = random.choice(MERCHANTS)
    country  = profile["preferred_country"]

    raw_amount = random.gauss(
        profile["typical_amount_mean"],
        profile["typical_amount_std"]
    )
    amount = round(max(0.50, raw_amount), 2)

    return Transaction(
        transaction_id    = f"txn_{uuid.uuid4().hex[:12]}",
        card_id           = card_id,
        user_id           = user_id,
        merchant_id       = merchant["merchant_id"],
        merchant_name     = merchant["merchant_name"],
        merchant_category = merchant["merchant_category"],
        amount            = amount,
        currency          = "USD",
        country           = country,
        city              = fake.city(),
        timestamp         = datetime.now(timezone.utc).isoformat(),
        is_international  = country != "US",
        card_present      = random.choice([True, True, True, False]),
        device_type       = random.choice(["mobile", "desktop", "pos", "pos", "pos"]),
    )


def _anomalous_transaction(user_id: str, card_id: str) -> Transaction:
    """
    Inject one of several anomaly patterns to test detection rules.
    """
    profile   = USER_PROFILES[user_id]
    anomaly   = random.choice([
        "large_amount",       # amount velocity rule
        "high_risk_merchant", # merchant category risk rule
        "off_hours",          # time-based rule
        "foreign_country",    # cross-border rule
    ])

    merchant = random.choice(MERCHANTS)
    country  = profile["preferred_country"]
    now      = datetime.now(timezone.utc)

    if anomaly == "large_amount":
        # 8–15x the user's typical amount
        amount = round(
            profile["typical_amount_mean"] * random.uniform(8, 15), 2
        )

    elif anomaly == "high_risk_merchant":
        hrc = random.choice(list(HIGH_RISK_MCC))
        merchant = {
            "merchant_id":       f"merch_risk_{uuid.uuid4().hex[:6]}",
            "merchant_name":     fake.company(),
            "merchant_category": hrc,
        }
        amount = round(random.uniform(200, 2000), 2)

    elif anomaly == "off_hours":
        # Simulate a 2am–4am transaction (replace hour in timestamp)
        off_hour = random.randint(0, 4)
        now = now.replace(hour=off_hour, minute=random.randint(0, 59))
        amount = round(random.uniform(300, 1500), 2)

    elif anomaly == "foreign_country":
        country = random.choice([c for c in ["CN", "RU", "NG", "BR", "MX"]
                                 if c != profile["preferred_country"]])
        amount  = round(random.uniform(100, 800), 2)

    else:
        amount = round(random.uniform(500, 5000), 2)

    return Transaction(
        transaction_id    = f"txn_{uuid.uuid4().hex[:12]}",
        card_id           = card_id,
        user_id           = user_id,
        merchant_id       = merchant.get("merchant_id", "merch_unknown"),
        merchant_name     = merchant.get("merchant_name", "Unknown Merchant"),
        merchant_category = merchant.get("merchant_category", "UNKNOWN"),
        amount            = amount,
        currency          = "USD",
        country           = country,
        city              = fake.city(),
        timestamp         = now.isoformat(),
        is_international  = country != "US",
        card_present      = False,
        device_type       = random.choice(["mobile", "desktop"]),
        _is_injected_anomaly = True,
    )


# ─── KAFKA PRODUCER ───────────────────────────────────────────────────────────

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers   = KAFKA_BOOTSTRAP_SERVERS,
        value_serializer    = lambda v: json.dumps(v).encode("utf-8"),
        key_serializer      = lambda k: k.encode("utf-8") if k else None,
        acks                = "all",           # strongest durability guarantee
        retries             = 5,
        linger_ms           = 5,               # micro-batch for throughput
        batch_size          = 32_768,          # 32 KB batch
        compression_type    = "gzip",
        max_in_flight_requests_per_connection = 1,  # preserve ordering
    )


def on_send_error(exc: Exception) -> None:
    print(f"[ERROR] Failed to send message: {exc}")


def run_producer() -> None:
    print(f"""
╔══════════════════════════════════════════════════════════╗
║         FinStream Transaction Producer                  ║
╠══════════════════════════════════════════════════════════╣
║  Kafka   : {KAFKA_BOOTSTRAP_SERVERS:<43} ║
║  Topic   : {KAFKA_TOPIC:<43} ║
║  TPS     : {TPS:<43} ║
║  Anomaly : {ANOMALY_RATE * 100:.0f}% injection rate{'':<30} ║
║  Users   : {NUM_USERS:<43} ║
║  Merchants: {NUM_MERCHANTS:<42} ║
╚══════════════════════════════════════════════════════════╝
""")

    producer = build_producer()

    # Pre-generate card IDs (1 card per user for simplicity)
    cards = {f"user_{i:05d}": f"card_{i:05d}" for i in range(NUM_USERS)}

    interval    = 1.0 / TPS
    sent_total  = 0
    anomaly_cnt = 0
    start_time  = time.time()

    print("[INFO] Producing transactions. Press Ctrl+C to stop.\n")

    try:
        while True:
            t0 = time.perf_counter()

            user_id = f"user_{random.randint(0, NUM_USERS - 1):05d}"
            card_id = cards[user_id]

            if random.random() < ANOMALY_RATE:
                txn = _anomalous_transaction(user_id, card_id)
                anomaly_cnt += 1
            else:
                txn = _normal_transaction(user_id, card_id)

            payload = asdict(txn)

            producer.send(
                KAFKA_TOPIC,
                key   = txn.card_id,        # partition by card for ordering
                value = payload,
            ).add_errback(on_send_error)

            sent_total += 1

            # Print stats every 10,000 messages
            if sent_total % 10_000 == 0:
                elapsed   = time.time() - start_time
                actual_tps = sent_total / elapsed
                print(
                    f"[INFO] Sent {sent_total:,} | "
                    f"Anomalies: {anomaly_cnt:,} ({anomaly_cnt/sent_total*100:.1f}%) | "
                    f"Actual TPS: {actual_tps:.0f}"
                )

            # Rate limiting
            elapsed_loop = time.perf_counter() - t0
            sleep_time   = interval - elapsed_loop
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print(f"\n[INFO] Stopping. Total sent: {sent_total:,} | Anomalies: {anomaly_cnt:,}")
    finally:
        producer.flush()
        producer.close()
        print("[INFO] Producer closed.")


if __name__ == "__main__":
    run_producer()
