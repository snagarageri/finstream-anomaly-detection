"""
Unit Tests — Anomaly Scoring Rules
====================================
Tests for the multi-rule anomaly scoring logic.
Validates that each rule fires correctly and composite scores
land in expected ranges.

Run: pytest tests/ -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timezone


# ─── FIXTURES ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("FinStream-Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def make_transaction(spark, overrides: dict = {}) -> "DataFrame":
    """Build a test transaction DataFrame with sensible defaults."""
    defaults = {
        "transaction_id":    "txn_test_001",
        "card_id":           "card_001",
        "user_id":           "user_001",
        "merchant_id":       "merch_001",
        "merchant_name":     "Test Merchant",
        "merchant_category": "GROCERY",
        "amount":            50.00,
        "currency":          "USD",
        "country":           "US",
        "city":              "New York",
        "event_time":        datetime(2025, 5, 12, 14, 30, 0, tzinfo=timezone.utc),
        "is_international":  False,
        "card_present":      True,
        "device_type":       "pos",
        "is_high_risk_mcc":  False,
        "is_cnp":            False,
        "is_off_hours":      False,
        "tx_hour":           14,
    }
    data = {**defaults, **overrides}
    return spark.createDataFrame([Row(**data)])


# ─── RULE 1: AMOUNT VELOCITY ──────────────────────────────────────────────────

class TestAmountVelocityRule:

    def test_normal_amount_score_near_zero(self, spark):
        """A typical $50 transaction among normal users should score near 0."""
        transactions = [
            Row(user_id="u1", amount=45.0),
            Row(user_id="u1", amount=55.0),
            Row(user_id="u1", amount=50.0),
            Row(user_id="u1", amount=48.0),
            Row(user_id="u1", amount=52.0),
        ]
        df = spark.createDataFrame(transactions)
        from pyspark.sql.window import Window
        w = Window.partitionBy("user_id")
        result = (
            df
            .withColumn("mean", F.mean("amount").over(w))
            .withColumn("std",  F.stddev("amount").over(w))
            .withColumn("zscore", (F.col("amount") - F.col("mean")) / F.col("std"))
            .withColumn("score", F.least(F.lit(100.0), F.greatest(F.lit(0.0), (F.col("zscore") / 6.0) * 100.0)))
        )
        scores = [r.score for r in result.collect()]
        assert all(s < 20 for s in scores), f"Normal transactions should score < 20, got {scores}"

    def test_large_amount_scores_high(self, spark):
        """A $5000 transaction when mean is $50 should score near 100."""
        transactions = [
            Row(user_id="u1", amount=50.0),
            Row(user_id="u1", amount=48.0),
            Row(user_id="u1", amount=52.0),
            Row(user_id="u1", amount=5000.0),   # anomaly
        ]
        df = spark.createDataFrame(transactions)
        from pyspark.sql.window import Window
        w = Window.partitionBy("user_id")
        result = (
            df
            .withColumn("mean", F.mean("amount").over(w))
            .withColumn("std",  F.stddev("amount").over(w))
            .withColumn("zscore",
                F.when(F.col("std") > 0,
                    (F.col("amount") - F.col("mean")) / F.col("std")
                ).otherwise(F.lit(0.0)))
            .withColumn("score", F.least(F.lit(100.0), F.greatest(F.lit(0.0), (F.col("zscore") / 6.0) * 100.0)))
            .filter(F.col("amount") == 5000.0)
        )
        high_score = result.first().score
        assert high_score >= 80, f"$5000 vs $50 mean should score >= 80, got {high_score}"

    def test_zero_std_doesnt_crash(self, spark):
        """All transactions equal → std = 0. Should score 0, not divide-by-zero."""
        transactions = [Row(user_id="u1", amount=50.0) for _ in range(5)]
        df = spark.createDataFrame(transactions)
        from pyspark.sql.window import Window
        w = Window.partitionBy("user_id")
        result = (
            df
            .withColumn("mean", F.mean("amount").over(w))
            .withColumn("std",  F.stddev("amount").over(w))
            .withColumn("zscore",
                F.when((F.col("std") > 0),
                    (F.col("amount") - F.col("mean")) / F.col("std")
                ).otherwise(F.lit(0.0)))
            .withColumn("score", F.least(F.lit(100.0), F.greatest(F.lit(0.0), (F.col("zscore") / 6.0) * 100.0)))
        )
        # Should not raise, all scores should be 0
        scores = [r.score for r in result.collect()]
        assert all(s == 0.0 for s in scores)


# ─── RULE 2: OFF-HOURS ────────────────────────────────────────────────────────

class TestOffHoursRule:

    @pytest.mark.parametrize("hour,expected_flag", [
        (0,  True),
        (1,  True),
        (3,  True),
        (4,  True),
        (5,  False),   # boundary: 5am is NOT off-hours
        (6,  False),
        (14, False),
        (23, False),
    ])
    def test_off_hours_flag(self, spark, hour, expected_flag):
        df = spark.createDataFrame([Row(tx_hour=hour)])
        result = df.withColumn(
            "is_off_hours",
            (F.col("tx_hour") >= 0) & (F.col("tx_hour") < 5)
        )
        flag = result.first().is_off_hours
        assert flag == expected_flag, f"Hour {hour}: expected {expected_flag}, got {flag}"

    def test_off_hours_scores_100(self, spark):
        df = spark.createDataFrame([Row(tx_hour=2, is_off_hours=True)])
        result = df.withColumn(
            "score_off_hours",
            F.when(F.col("is_off_hours"), F.lit(100.0)).otherwise(F.lit(0.0))
        )
        assert result.first().score_off_hours == 100.0

    def test_business_hours_scores_zero(self, spark):
        df = spark.createDataFrame([Row(tx_hour=14, is_off_hours=False)])
        result = df.withColumn(
            "score_off_hours",
            F.when(F.col("is_off_hours"), F.lit(100.0)).otherwise(F.lit(0.0))
        )
        assert result.first().score_off_hours == 0.0


# ─── RULE 3: HIGH-RISK MCC ───────────────────────────────────────────────────

class TestHighRiskMCCRule:

    HIGH_RISK = ["CASH_ADVANCE", "GAMBLING", "CRYPTOCURRENCY"]
    LOW_RISK  = ["GROCERY", "RESTAURANT", "PHARMACY", "GAS_STATION"]

    @pytest.mark.parametrize("mcc", HIGH_RISK)
    def test_high_risk_mcc_scores_100(self, spark, mcc):
        df = spark.createDataFrame([Row(merchant_category=mcc)])
        result = df.withColumn(
            "score_mcc",
            F.when(F.col("merchant_category").isin(self.HIGH_RISK), F.lit(100.0))
            .otherwise(F.lit(0.0))
        )
        assert result.first().score_mcc == 100.0, f"{mcc} should score 100"

    @pytest.mark.parametrize("mcc", LOW_RISK)
    def test_normal_mcc_scores_zero(self, spark, mcc):
        df = spark.createDataFrame([Row(merchant_category=mcc)])
        result = df.withColumn(
            "score_mcc",
            F.when(F.col("merchant_category").isin(self.HIGH_RISK), F.lit(100.0))
            .otherwise(F.lit(0.0))
        )
        assert result.first().score_mcc == 0.0, f"{mcc} should score 0"


# ─── COMPOSITE SCORE TESTS ────────────────────────────────────────────────────

class TestCompositeScore:

    def test_all_rules_fire_scores_100(self, spark):
        """
        If every rule fires at max, composite score should be 100.
        """
        df = spark.createDataFrame([Row(
            score_amount_velocity = 100.0,
            score_tx_frequency    = 100.0,
            score_off_hours       = 100.0,
            score_cross_border    = 100.0,
            score_high_risk_mcc   = 100.0,
        )])
        result = df.withColumn(
            "anomaly_score",
            F.round(
                F.col("score_amount_velocity")  * 0.35 +
                F.col("score_tx_frequency")     * 0.25 +
                F.col("score_off_hours")        * 0.15 +
                F.col("score_cross_border")     * 0.15 +
                F.col("score_high_risk_mcc")    * 0.10,
                2
            )
        )
        assert result.first().anomaly_score == 100.0

    def test_no_rules_fire_scores_zero(self, spark):
        df = spark.createDataFrame([Row(
            score_amount_velocity = 0.0,
            score_tx_frequency    = 0.0,
            score_off_hours       = 0.0,
            score_cross_border    = 0.0,
            score_high_risk_mcc   = 0.0,
        )])
        result = df.withColumn(
            "anomaly_score",
            F.round(
                F.col("score_amount_velocity")  * 0.35 +
                F.col("score_tx_frequency")     * 0.25 +
                F.col("score_off_hours")        * 0.15 +
                F.col("score_cross_border")     * 0.15 +
                F.col("score_high_risk_mcc")    * 0.10,
                2
            )
        )
        assert result.first().anomaly_score == 0.0

    @pytest.mark.parametrize("score,expected_level", [
        (15.0,  "LOW"),
        (39.9,  "LOW"),
        (40.0,  "MEDIUM"),
        (69.9,  "MEDIUM"),
        (70.0,  "HIGH"),
        (100.0, "HIGH"),
    ])
    def test_risk_level_thresholds(self, spark, score, expected_level):
        df = spark.createDataFrame([Row(anomaly_score=score)])
        result = df.withColumn(
            "risk_level",
            F.when(F.col("anomaly_score") >= 70.0, F.lit("HIGH"))
            .when(F.col("anomaly_score") >= 40.0, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW"))
        )
        assert result.first().risk_level == expected_level

    def test_score_always_between_0_and_100(self, spark):
        """Score must always be in [0, 100] regardless of input values."""
        extreme_cases = [
            Row(score_amount_velocity=200.0, score_tx_frequency=200.0,
                score_off_hours=200.0, score_cross_border=200.0, score_high_risk_mcc=200.0),
            Row(score_amount_velocity=-50.0, score_tx_frequency=-10.0,
                score_off_hours=-5.0, score_cross_border=-20.0, score_high_risk_mcc=-1.0),
        ]
        df = spark.createDataFrame(extreme_cases)
        result = df.withColumn(
            "raw_score",
            F.col("score_amount_velocity")  * 0.35 +
            F.col("score_tx_frequency")     * 0.25 +
            F.col("score_off_hours")        * 0.15 +
            F.col("score_cross_border")     * 0.15 +
            F.col("score_high_risk_mcc")    * 0.10
        ).withColumn(
            "anomaly_score",
            F.least(F.lit(100.0), F.greatest(F.lit(0.0), F.col("raw_score")))
        )
        for row in result.collect():
            assert 0.0 <= row.anomaly_score <= 100.0, \
                f"Score {row.anomaly_score} is out of [0, 100] bounds"
