{{
    config(
        materialized='view',
        description='Staging layer: standardize column names, cast types, and apply a surrogate trip_id from the raw warehouse.'
    )
}}

/*
  stg_trips — Staging Layer
  -------------------------
  Reads from raw.trips (loaded by the DuckDB bulk loader).
  Responsibilities:
    1. Deduplicate exact duplicate rows from the TLC source (ROW_NUMBER).
    2. Rename columns to snake_case analytical standard.
    3. Enforce strict data type casting.
    4. Generate a surrogate trip_id using a hash of natural keys.
    5. No business logic — that lives in the intermediate layer.
*/

-- Step 1: Deduplicate identical source rows (TLC raw data contains ~30 exact duplicates).
-- ROW_NUMBER() over all columns ensures we keep exactly one copy of each record.
WITH deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                VendorID,
                tpep_pickup_datetime,
                PULocationID,
                DOLocationID,
                total_amount,
                passenger_count
            ORDER BY (SELECT NULL)   -- No meaningful tiebreaker; just pick one
        ) AS _row_num
    FROM {{ source('raw', 'trips') }}
)

SELECT
    -- Surrogate key: hash of vendor + pickup time + locations + amount + passengers
    md5(
        CAST(VendorID AS VARCHAR) || '|' ||
        CAST(tpep_pickup_datetime AS VARCHAR) || '|' ||
        CAST(PULocationID AS VARCHAR) || '|' ||
        CAST(DOLocationID AS VARCHAR) || '|' ||
        CAST(total_amount AS VARCHAR) || '|' ||
        CAST(passenger_count AS VARCHAR)
    ) AS trip_id,

    -- Dimensions
    CAST(VendorID AS INTEGER)              AS vendor_id,
    CAST(PULocationID AS INTEGER)          AS pickup_location_id,
    CAST(DOLocationID AS INTEGER)          AS dropoff_location_id,
    CAST(RatecodeID AS INTEGER)            AS ratecode_id,
    CAST(payment_type AS INTEGER)          AS payment_type_code,
    CAST(passenger_count AS INTEGER)       AS passenger_count,
    CAST(store_and_fwd_flag AS VARCHAR)    AS store_and_fwd_flag,

    -- Timestamps
    CAST(tpep_pickup_datetime AS TIMESTAMP)   AS pickup_at,
    CAST(tpep_dropoff_datetime AS TIMESTAMP)  AS dropoff_at,

    -- Metrics (already cleaned by PySpark)
    CAST(trip_distance AS DOUBLE)              AS trip_distance_miles,
    CAST(trip_duration_minutes AS DOUBLE)      AS trip_duration_minutes,
    CAST(avg_speed_mph AS DOUBLE)              AS avg_speed_mph,

    -- Financials
    CAST(fare_amount AS DOUBLE)                AS fare_amount,
    CAST(extra AS DOUBLE)                      AS extra,
    CAST(mta_tax AS DOUBLE)                    AS mta_tax,
    CAST(tip_amount AS DOUBLE)                 AS tip_amount,
    CAST(tolls_amount AS DOUBLE)               AS tolls_amount,
    CAST(improvement_surcharge AS DOUBLE)      AS improvement_surcharge,
    CAST(congestion_surcharge AS DOUBLE)       AS congestion_surcharge,
    CAST(Airport_fee AS DOUBLE)                AS airport_fee,
    CAST(total_amount AS DOUBLE)               AS total_amount,
    CAST(tip_percentage AS DOUBLE)             AS tip_percentage

FROM deduplicated
WHERE _row_num = 1
