{{
  config(
    materialized    = 'incremental',
    unique_key      = 'transaction_id',
    incremental_strategy = 'append',
    tags            = ['bronze', 'raw'],
    meta = {
      'layer': 'bronze',
      'description': 'Raw immutable transaction records ingested from Delta Lake. No transformations applied. Append-only.'
    }
  )
}}

/*
  Bronze Layer — Raw Transactions
  ================================
  Source: Delta Lake /delta/bronze/transactions
           (written by PySpark Structured Streaming job)

  This model reads raw parsed records directly from the Delta Lake
  bronze path. No business logic is applied here — the goal is
  immutability and complete lineage from source.

  New records only (incremental append using ingested_at watermark).
*/

SELECT
    -- Core transaction identity
    transaction_id,
    card_id,
    user_id,
    merchant_id,
    merchant_name,
    merchant_category,

    -- Financial fields
    CAST(amount AS DOUBLE)      AS amount,
    currency,

    -- Location
    country,
    city,

    -- Event timing
    CAST(timestamp AS TIMESTAMP) AS event_timestamp,

    -- Flags from source
    CAST(is_international AS BOOLEAN) AS is_international,
    CAST(card_present     AS BOOLEAN) AS card_present,
    device_type,

    -- Pipeline metadata
    CAST(ingested_at AS TIMESTAMP)      AS ingested_at,
    CAST(kafka_ingest_time AS TIMESTAMP) AS kafka_ingest_time,
    kafka_offset,
    kafka_partition,
    pipeline_version,

    -- Audit columns
    CURRENT_TIMESTAMP() AS dbt_loaded_at,
    '{{ invocation_id }}' AS dbt_run_id

FROM delta.`{{ var('delta_base_path') }}/bronze/transactions`

{% if is_incremental() %}
WHERE ingested_at > (
    SELECT COALESCE(MAX(ingested_at), '1970-01-01') FROM {{ this }}
)
{% endif %}
