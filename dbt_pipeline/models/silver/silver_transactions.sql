{{
  config(
    materialized         = 'incremental',
    unique_key           = 'transaction_id',
    incremental_strategy = 'merge',
    tags                 = ['silver', 'cleaned'],
    meta = {
      'layer': 'silver',
      'description': 'Cleaned, typed, deduplicated, and enriched transaction records. Business logic applied.'
    }
  )
}}

/*
  Silver Layer — Cleaned & Enriched Transactions
  ================================================
  Source: {{ ref('bronze_transactions') }}

  Applies:
  - Deduplication on transaction_id
  - String normalization (trim, upper)
  - Derived time features
  - Amount bucketing
  - Merchant risk classification
  - Card-not-present flag
  - Off-hours detection
*/

WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ ref('bronze_transactions') }}
    WHERE
        transaction_id IS NOT NULL
        AND card_id IS NOT NULL
        AND amount IS NOT NULL
        AND amount > 0.00
        AND amount <= 100000.00   -- sanity cap
        AND currency IN ('USD', 'EUR', 'GBP', 'CAD', 'JPY', 'AUD')
),

cleaned AS (
    SELECT
        transaction_id,
        card_id,
        user_id,
        merchant_id,
        TRIM(merchant_name)                 AS merchant_name,
        UPPER(TRIM(merchant_category))      AS merchant_category,
        ROUND(amount, 2)                    AS amount,
        UPPER(TRIM(currency))               AS currency,
        UPPER(TRIM(country))                AS country,
        TRIM(city)                          AS city,
        event_timestamp,
        is_international,
        card_present,
        LOWER(TRIM(device_type))            AS device_type,
        ingested_at,
        kafka_ingest_time,
        kafka_offset,
        kafka_partition,
        pipeline_version,

        -- ── Time Features ────────────────────────────────────────────────
        HOUR(event_timestamp)               AS tx_hour,
        DAYOFWEEK(event_timestamp)          AS tx_day_of_week,   -- 1=Sun
        DATE(event_timestamp)               AS tx_date,
        WEEKOFYEAR(event_timestamp)         AS tx_week,
        MONTH(event_timestamp)              AS tx_month,

        -- ── Amount Bucketing ─────────────────────────────────────────────
        CASE
            WHEN amount < 25     THEN 'MICRO'
            WHEN amount < 100    THEN 'SMALL'
            WHEN amount < 500    THEN 'MEDIUM'
            WHEN amount < 2000   THEN 'LARGE'
            ELSE                      'VERY_LARGE'
        END                                 AS amount_bucket,

        -- ── Risk Flags ───────────────────────────────────────────────────
        CASE
            WHEN UPPER(TRIM(merchant_category)) IN (
                'CASH_ADVANCE', 'GAMBLING', 'CRYPTOCURRENCY'
            ) THEN TRUE ELSE FALSE
        END                                 AS is_high_risk_mcc,

        NOT card_present                    AS is_cnp,

        CASE
            WHEN HOUR(event_timestamp) BETWEEN 0 AND 4 THEN TRUE
            ELSE FALSE
        END                                 AS is_off_hours,

        CASE
            WHEN UPPER(TRIM(country)) != 'US' THEN TRUE
            ELSE FALSE
        END                                 AS is_foreign_country,

        -- ── Processing Latency ───────────────────────────────────────────
        DATEDIFF(MILLISECOND, event_timestamp, kafka_ingest_time)
                                            AS kafka_latency_ms,

        CURRENT_TIMESTAMP()                 AS dbt_loaded_at

    FROM deduped
    WHERE rn = 1

    {% if is_incremental() %}
    AND ingested_at > (
        SELECT COALESCE(MAX(ingested_at), '1970-01-01') FROM {{ this }}
    )
    {% endif %}
)

SELECT * FROM cleaned
