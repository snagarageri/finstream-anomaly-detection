# 🔍 FinStream — Real-Time Financial Transaction Anomaly Detection Pipeline

[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=flat&logo=delta&logoColor=white)](https://delta.io/)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Grafana](https://img.shields.io/badge/Grafana-F46800?style=flat&logo=grafana&logoColor=white)](https://grafana.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://python.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://docker.com/)

---

## 📌 Overview

**FinStream** is a production-grade, end-to-end real-time data engineering pipeline that ingests synthetic credit card transaction streams, detects anomalies using multi-rule scoring logic in PySpark Structured Streaming, persists data in a Delta Lake Medallion Architecture (Bronze → Silver → Gold), transforms analytics-ready models with dbt, and surfaces live fraud signals on a Grafana dashboard.

This project mirrors real-world architectures used by financial institutions (Mastercard, Visa, JPMorgan) for transaction fraud monitoring at scale.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FINSTREAM PIPELINE ARCHITECTURE                     │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────┐
  │  Python Producer │  ← Faker-based synthetic transaction generator
  │  (Kafka Client)  │    500–1,000 events/sec | schema-validated JSON
  └────────┬─────────┘
           │  Kafka Topic: raw-transactions
           ▼
  ┌──────────────────┐
  │   Apache Kafka   │  ← Message broker | 3 partitions | 7-day retention
  │  + Zookeeper     │    Offset management | consumer group tracking
  └────────┬─────────┘
           │  Structured Streaming read
           ▼
  ┌────────────────────────────────────────────────────────┐
  │              PySpark Structured Streaming               │
  │                                                        │
  │  ┌──────────────┐  ┌────────────────┐  ┌───────────┐  │
  │  │ Schema Parse │→ │ Anomaly Scorer │→ │ Watermark │  │
  │  └──────────────┘  └────────────────┘  └─────┬─────┘  │
  │                                               │        │
  │  Anomaly Rules:                               │        │
  │  • Amount velocity (z-score > 3σ)             │        │
  │  • Transaction frequency (>5 in 10 min)       │        │
  │  • Off-hours activity (midnight–5am)          │        │
  │  • Cross-border rapid spend (<1hr)            │        │
  │  • New merchant category spike               │        │
  └───────────────────────────────────────────────┼────────┘
                                                  │
           ┌──────────────────────────────────────┘
           │  Delta Lake writes (ACID transactions)
           ▼
  ┌─────────────────────────────────────────────────────────┐
  │                  DELTA LAKE  (local / S3)               │
  │                                                         │
  │  🥉 Bronze Layer   →  Raw, immutable, append-only       │
  │     /delta/bronze/transactions                          │
  │                                                         │
  │  🥈 Silver Layer   →  Cleaned, typed, deduplicated      │
  │     /delta/silver/transactions                          │
  │     /delta/silver/user_stats (rolling aggregates)       │
  │                                                         │
  │  🥇 Gold Layer     →  Business-ready, anomaly-flagged   │
  │     /delta/gold/anomalies                               │
  │     /delta/gold/transaction_summary                     │
  └──────────────────────────┬──────────────────────────────┘
                             │  dbt models
                             ▼
  ┌──────────────────┐     ┌────────────────────┐
  │   dbt Core       │────▶│  DuckDB / Postgres  │  ← analytics store
  │   (transforms)   │     │  (query engine)     │
  └──────────────────┘     └────────────┬────────┘
                                        │
                                        ▼
  ┌──────────────────┐     ┌────────────────────┐
  │ Apache Airflow   │     │      Grafana        │  ← Live dashboard
  │ (orchestration)  │     │  • Anomaly heatmap  │
  │                  │     │  • Velocity gauge   │
  └──────────────────┘     │  • Risk score trend │
                           └────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Make

### 1. Clone & Configure
```bash
git clone https://github.com/YOUR_USERNAME/finstream-anomaly-detection.git
cd finstream-anomaly-detection
cp .env.example .env
```

### 2. Start All Services
```bash
make up
```
This starts: Kafka, Zookeeper, Spark, Delta Lake storage, PostgreSQL, Airflow, Grafana.

### 3. Start the Transaction Producer
```bash
make produce
# Starts generating 500–1,000 synthetic transactions/sec into Kafka
```

### 4. Start the Spark Streaming Job
```bash
make stream
# PySpark reads from Kafka, scores anomalies, writes to Delta Lake
```

### 5. Run dbt Transformations
```bash
make dbt-run
# Builds Bronze → Silver → Gold models
```

### 6. Open Grafana Dashboard
```
http://localhost:3000
Username: admin  |  Password: finstream
```

---

## 📁 Project Structure

```
finstream-anomaly-detection/
│
├── 📂 producer/                      # Kafka transaction generator
│   ├── transaction_generator.py      # Synthetic data producer (Faker)
│   ├── schema.py                     # Pydantic transaction schema
│   ├── requirements.txt
│   └── Dockerfile
│
├── 📂 spark_streaming/               # PySpark anomaly detection engine
│   ├── anomaly_detector.py           # Main streaming job
│   ├── rules/
│   │   ├── velocity_rule.py          # Amount velocity scoring
│   │   ├── frequency_rule.py         # Transaction frequency scoring
│   │   ├── time_rule.py              # Off-hours scoring
│   │   └── geo_rule.py              # Cross-border scoring
│   ├── schema.py                     # Spark schema definitions
│   ├── requirements.txt
│   └── Dockerfile
│
├── 📂 dbt_pipeline/                  # dbt transformation project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── bronze/                   # Raw ingestion models
│       ├── silver/                   # Cleaned & enriched models
│       └── gold/                     # Business-ready anomaly models
│
├── 📂 grafana/                       # Dashboard definitions
│   ├── dashboards/transactions.json
│   └── provisioning/
│
├── 📂 airflow/dags/                  # Orchestration DAGs
│   └── pipeline_dag.py
│
├── 📂 notebooks/
│   └── exploration.ipynb             # EDA and rule validation
│
├── 📂 .github/workflows/             # CI/CD
│   └── ci.yml
│
├── docker-compose.yml
├── Makefile
├── .env.example
└── README.md
```

---

## 🔬 Anomaly Detection Logic

Each transaction is scored 0–100 using a weighted multi-rule system:

| Rule | Weight | Trigger Condition |
|------|--------|-------------------|
| **Amount Velocity** | 35% | Transaction amount > 3σ above user's 30-day rolling mean |
| **Transaction Frequency** | 25% | >5 transactions from same card within a 10-minute window |
| **Off-Hours Activity** | 15% | Transaction between 00:00–05:00 local time |
| **Cross-Border Speed** | 15% | Same card used in 2 different countries within 60 minutes |
| **Merchant Category Spike** | 10% | First-ever transaction in a high-risk MCC for this user |

**Score thresholds:**
- `0–39` → ✅ Normal
- `40–69` → ⚠️ Suspicious (alert)
- `70–100` → 🚨 Anomaly (block + alert)

---

## 📊 Data Schema

### Kafka Message (raw-transactions topic)
```json
{
  "transaction_id": "txn_a3f91b2c",
  "card_id": "card_00042",
  "user_id": "user_00391",
  "merchant_id": "merch_00817",
  "merchant_name": "Amazon",
  "merchant_category": "ONLINE_RETAIL",
  "amount": 247.83,
  "currency": "USD",
  "country": "US",
  "city": "New York",
  "timestamp": "2025-05-12T14:23:01.442Z",
  "is_international": false,
  "card_present": true,
  "device_type": "mobile"
}
```

### Gold Layer — Anomaly Record
```json
{
  "transaction_id": "txn_a3f91b2c",
  "anomaly_score": 78.5,
  "risk_level": "HIGH",
  "triggered_rules": ["amount_velocity", "off_hours"],
  "amount_zscore": 3.8,
  "tx_count_10min": 2,
  "user_30d_mean": 62.14,
  "user_30d_stddev": 48.21,
  "flagged_at": "2025-05-12T14:23:01.889Z",
  "processing_latency_ms": 447
}
```

---

## 🧪 Data Quality — dbt Tests

```yaml
# Every model has automated quality gates
models:
  - name: silver_transactions
    tests:
      - not_null: [transaction_id, card_id, amount, timestamp]
      - unique: [transaction_id]
      - accepted_values:
          column_name: currency
          values: ['USD', 'EUR', 'GBP', 'CAD', 'JPY']
      - dbt_expectations.expect_column_values_to_be_between:
          column_name: amount
          min_value: 0.01
          max_value: 100000

  - name: gold_anomalies
    tests:
      - not_null: [anomaly_score, risk_level, triggered_rules]
      - dbt_expectations.expect_column_values_to_be_between:
          column_name: anomaly_score
          min_value: 0
          max_value: 100
```

---

## ⚡ Performance Benchmarks

| Metric | Value |
|--------|-------|
| Throughput | ~1,200 transactions/sec |
| End-to-end latency (Kafka → Gold) | < 800ms |
| Anomaly scoring latency | < 50ms per record |
| Delta Lake write batch interval | 30 seconds |
| dbt model build time (full refresh) | ~45 seconds |
| Grafana refresh interval | 10 seconds |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| Message Broker | Apache Kafka 3.6 + Zookeeper |
| Stream Processing | PySpark 3.5 Structured Streaming |
| Storage | Delta Lake 3.0 (local / AWS S3 compatible) |
| Transformation | dbt Core 1.7 + dbt-duckdb adapter |
| Orchestration | Apache Airflow 2.8 |
| Serving DB | DuckDB / PostgreSQL |
| Visualization | Grafana 10.x |
| Containerization | Docker + Docker Compose |
| Language | Python 3.11 |
| CI/CD | GitHub Actions |

---

## 📈 Grafana Dashboard Panels

1. **Live Transaction Feed** — Real-time table of last 100 transactions
2. **Anomaly Rate Gauge** — % of flagged transactions (rolling 5-min)
3. **Risk Score Heatmap** — Score distribution by hour of day
4. **Top Flagged Cards** — Bar chart of highest-risk card IDs
5. **Transaction Velocity** — Time series of tx/sec by merchant category
6. **Alert Timeline** — Chronological HIGH/MEDIUM anomaly events

---

## 🔄 CI/CD Pipeline

On every push to `main`:
1. ✅ Run unit tests (pytest) on anomaly scoring rules
2. ✅ Validate dbt models compile without errors
3. ✅ Run dbt tests against sample fixtures
4. ✅ Build and push Docker images
5. ✅ Lint Python with flake8 + black

---

## 📚 Key Concepts Demonstrated

- **Medallion Architecture** — Bronze/Silver/Gold data lake pattern
- **Exactly-Once Semantics** — Kafka + Spark checkpoint-based processing
- **Watermarking** — Handling late-arriving data in streaming
- **Delta Lake ACID** — Concurrent reads and writes, time travel queries
- **dbt Data Contracts** — Schema tests + data quality enforcement
- **Stateful Streaming** — Rolling window aggregates per user/card
- **Schema Evolution** — Forward-compatible Kafka message schemas

---

## 🤝 Author

**Saichitra Nagarageri** — Senior Data Engineer  
[LinkedIn](https://www.linkedin.com/in/nagarageri-saichitra/) | [Email](mailto:saichitrareddy13@gmail.com)

---

*Built to demonstrate production-grade real-time data engineering patterns used in financial services.*
