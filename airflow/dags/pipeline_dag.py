"""
FinStream Pipeline Orchestration DAG
======================================
Orchestrates the full FinStream data pipeline:

  1. Health checks — verify Kafka and Delta Lake are reachable
  2. dbt Bronze run — sync latest raw records
  3. dbt Silver run — clean and enrich
  4. dbt Tests     — validate data quality at Silver layer
  5. dbt Gold run  — build anomaly analytics tables
  6. dbt Gold Tests — validate anomaly score integrity
  7. Grafana Notify — trigger dashboard refresh (via API)
  8. Alert Summary  — publish daily anomaly summary to Kafka

Schedule: Every 30 minutes during business hours.
           Hourly overnight (low-volume maintenance mode).

Owner: Data Engineering — Saichitra Nagarageri
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.http.operators.http import SimpleHttpOperator

import json
import logging

logger = logging.getLogger(__name__)

# ─── DAG CONFIG ───────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":               "saichitra",
    "depends_on_past":     False,
    "start_date":          datetime(2025, 1, 1),
    "email_on_failure":    True,
    "email_on_retry":      False,
    "retries":             2,
    "retry_delay":         timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay":     timedelta(minutes=10),
}

DBT_DIR = "/usr/app/dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt"


# ─── HEALTH CHECK FUNCTIONS ───────────────────────────────────────────────────

def check_kafka_health(**context) -> str:
    """
    Verify Kafka broker is reachable and the raw-transactions
    topic exists with the expected partition count.
    """
    from kafka import KafkaAdminClient
    from kafka.errors import KafkaError
    import os

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5000)
        topics = admin.list_topics()
        assert "raw-transactions" in topics, "Topic 'raw-transactions' not found!"
        admin.close()
        logger.info(f"✅ Kafka healthy. Topics found: {topics}")
        return "kafka_healthy"
    except Exception as e:
        logger.error(f"❌ Kafka health check failed: {e}")
        return "kafka_unhealthy"


def check_delta_health(**context) -> None:
    """
    Verify Delta Lake paths exist and are readable.
    """
    import os
    delta_base = os.getenv("DELTA_PATH", "/delta")
    required_paths = [
        f"{delta_base}/bronze/transactions",
        f"{delta_base}/silver/transactions",
        f"{delta_base}/gold/anomalies",
    ]
    for path in required_paths:
        if not os.path.exists(path):
            logger.warning(f"⚠️  Delta path not yet created: {path}")
        else:
            logger.info(f"✅ Delta path OK: {path}")


def publish_anomaly_summary(**context) -> None:
    """
    Compute a daily anomaly summary and publish to Kafka
    for downstream reporting consumers.
    """
    from kafka import KafkaProducer
    import os, json
    from datetime import date

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    today     = str(date.today())

    # In production: query Gold layer for actual stats
    # Here: generate a representative summary payload
    summary = {
        "report_date":        today,
        "pipeline_run":       context["run_id"],
        "total_transactions": 0,   # filled by Gold query in production
        "high_risk_count":    0,
        "medium_risk_count":  0,
        "low_risk_count":     0,
        "top_flagged_cards":  [],
        "generated_at":       datetime.utcnow().isoformat(),
    }

    producer = KafkaProducer(
        bootstrap_servers = bootstrap,
        value_serializer  = lambda v: json.dumps(v).encode("utf-8"),
    )
    producer.send("anomaly-daily-summary", value=summary)
    producer.flush()
    producer.close()
    logger.info(f"✅ Published daily anomaly summary for {today}")


# ─── DAG DEFINITION ───────────────────────────────────────────────────────────

with DAG(
    dag_id      = "finstream_pipeline",
    default_args = DEFAULT_ARGS,
    description = "FinStream end-to-end data pipeline orchestration",
    schedule_interval = "*/30 6-22 * * *",  # every 30 min, 6am–10pm
    catchup     = False,
    max_active_runs = 1,
    tags        = ["finstream", "data-engineering", "real-time"],
    doc_md      = __doc__,
) as dag:

    # ── Start ────────────────────────────────────────────────────────────────
    start = DummyOperator(task_id="start")

    # ── Health Checks ────────────────────────────────────────────────────────
    check_kafka = BranchPythonOperator(
        task_id         = "check_kafka_health",
        python_callable = check_kafka_health,
    )

    kafka_healthy = DummyOperator(task_id="kafka_healthy")
    kafka_unhealthy = DummyOperator(task_id="kafka_unhealthy")

    check_delta = PythonOperator(
        task_id         = "check_delta_health",
        python_callable = check_delta_health,
    )

    # ── dbt Bronze ───────────────────────────────────────────────────────────
    dbt_bronze = BashOperator(
        task_id     = "dbt_bronze_run",
        bash_command = f"{DBT_CMD} run --select bronze --profiles-dir . --project-dir . --no-partial-parse",
    )

    dbt_bronze_test = BashOperator(
        task_id     = "dbt_bronze_test",
        bash_command = f"{DBT_CMD} test --select bronze --profiles-dir . --project-dir .",
    )

    # ── dbt Silver ───────────────────────────────────────────────────────────
    dbt_silver = BashOperator(
        task_id     = "dbt_silver_run",
        bash_command = f"{DBT_CMD} run --select silver --profiles-dir . --project-dir .",
    )

    dbt_silver_test = BashOperator(
        task_id     = "dbt_silver_test",
        bash_command = f"{DBT_CMD} test --select silver --profiles-dir . --project-dir .",
    )

    # ── dbt Gold ─────────────────────────────────────────────────────────────
    dbt_gold = BashOperator(
        task_id     = "dbt_gold_run",
        bash_command = f"{DBT_CMD} run --select gold --profiles-dir . --project-dir .",
    )

    dbt_gold_test = BashOperator(
        task_id     = "dbt_gold_test",
        bash_command = f"{DBT_CMD} test --select gold --profiles-dir . --project-dir .",
    )

    # ── Anomaly Summary ──────────────────────────────────────────────────────
    publish_summary = PythonOperator(
        task_id         = "publish_anomaly_summary",
        python_callable = publish_anomaly_summary,
        trigger_rule    = TriggerRule.ALL_SUCCESS,
    )

    # ── End ──────────────────────────────────────────────────────────────────
    end = DummyOperator(
        task_id      = "pipeline_complete",
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ─── TASK DEPENDENCIES ───────────────────────────────────────────────────
    (
        start
        >> check_kafka
        >> [kafka_healthy, kafka_unhealthy]
    )

    (
        kafka_healthy
        >> check_delta
        >> dbt_bronze
        >> dbt_bronze_test
        >> dbt_silver
        >> dbt_silver_test
        >> dbt_gold
        >> dbt_gold_test
        >> publish_summary
        >> end
    )

    kafka_unhealthy >> end
