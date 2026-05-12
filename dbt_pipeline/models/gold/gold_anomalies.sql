{{
  config(
    materialized = 'table',
    tags         = ['gold', 'analytics', 'anomalies'],
    meta = {
      'layer': 'gold',
      'description': 'Business-ready anomaly records with multi-rule scores. Powers Grafana dashboards and alerting.'
    }
  )
}}

/*
  Gold Layer — Anomaly Records
  ==============================
  Source: {{ ref('silver_transactions') }} + Delta Lake gold anomaly scores

  Joins the Spark-computed anomaly scores (written to Delta Lake gold path)
  with the clean silver transaction records to produce the final
  analytics-ready table. Filters to MEDIUM and HIGH risk only.

  This is the primary table powering:
    - Grafana real-time dashboard
    - Downstream case management system
    - Executive fraud reporting
*/

WITH spark_scores AS (
    -- Read anomaly scores produced by PySpark Structured Streaming
    SELECT
        transaction_id,
        ROUND(anomaly_score, 2)                 AS anomaly_score,
        risk_level,
        triggered_rules,
        ROUND(amount_zscore, 4)                 AS amount_zscore,
        ROUND(score_amount_velocity, 2)         AS score_amount_velocity,
        ROUND(score_tx_frequency, 2)            AS score_tx_frequency,
        ROUND(score_off_hours, 2)               AS score_off_hours,
        ROUND(score_cross_border, 2)            AS score_cross_border,
        ROUND(score_high_risk_mcc, 2)           AS score_high_risk_mcc,
        scored_at
    FROM delta.`{{ var('delta_base_path') }}/gold/anomalies`
    WHERE risk_level IN ('MEDIUM', 'HIGH')
),

enriched AS (
    SELECT
        -- Transaction identity
        t.transaction_id,
        t.card_id,
        t.user_id,
        t.merchant_id,
        t.merchant_name,
        t.merchant_category,

        -- Financial
        t.amount,
        t.currency,
        t.amount_bucket,

        -- Location
        t.country,
        t.city,
        t.is_international,
        t.is_foreign_country,

        -- Device
        t.card_present,
        t.is_cnp,
        t.device_type,

        -- Time
        t.event_timestamp,
        t.tx_hour,
        t.tx_day_of_week,
        t.tx_date,
        t.is_off_hours,

        -- Risk signals
        t.is_high_risk_mcc,

        -- Anomaly scores (from Spark)
        s.anomaly_score,
        s.risk_level,
        s.triggered_rules,
        s.amount_zscore,
        s.score_amount_velocity,
        s.score_tx_frequency,
        s.score_off_hours,
        s.score_cross_border,
        s.score_high_risk_mcc,

        -- Categorical risk label
        CASE
            WHEN s.anomaly_score >= {{ var('anomaly_threshold') }}    THEN '🚨 ANOMALY'
            WHEN s.anomaly_score >= {{ var('suspicious_threshold') }} THEN '⚠️  SUSPICIOUS'
            ELSE                                                           '✅ NORMAL'
        END                                     AS risk_label,

        -- Timing
        s.scored_at,
        DATEDIFF(
            MILLISECOND, t.event_timestamp, s.scored_at
        )                                       AS end_to_end_latency_ms,

        CURRENT_TIMESTAMP()                     AS dbt_loaded_at

    FROM {{ ref('silver_transactions') }} t
    INNER JOIN spark_scores s
        ON t.transaction_id = s.transaction_id
)

SELECT * FROM enriched
ORDER BY anomaly_score DESC, event_timestamp DESC
