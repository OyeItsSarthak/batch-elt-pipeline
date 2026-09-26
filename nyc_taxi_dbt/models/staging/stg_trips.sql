/*
  Model: stg_trips
  Layer: Staging
  Source: raw.trips (loaded by PySpark + DuckDB bulk loader)

  Purpose:
    - Standardizes raw column names to snake_case conventions.
    - Enforces explicit data types and adds surrogate row identifiers.
    - Applies final safety filters (removes rows that slipped past the Spark job).
    - Serves as the single source of truth for all downstream trip models.
*/

WITH source AS (
    SELECT * FROM raw.trips
),

renamed AS (
    SELECT
        -- Identifiers
        VendorID                                        AS vendor_id,
        PULocationID                                    AS pickup_location_id,
        DOLocationID                                    AS dropoff_location_id,

        -- Timestamps
        tpep_pickup_datetime                            AS pickup_at,
        tpep_dropoff_datetime                           AS dropoff_at,

        -- Trip characteristics
        passenger_count,
        trip_distance,
        trip_duration_minutes,
        avg_speed_mph,
        RatecodeID                                      AS rate_code_id,
        store_and_fwd_flag,

        -- Financials
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tip_percentage,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        Airport_fee                                     AS airport_fee,
        total_amount,
        payment_type
    FROM source
),

final AS (
    SELECT
        -- Stable surrogate key using a hash of business key columns
        md5(
            vendor_id::VARCHAR || '|' ||
            pickup_at::VARCHAR || '|' ||
            dropoff_at::VARCHAR || '|' ||
            pickup_location_id::VARCHAR || '|' ||
            dropoff_location_id::VARCHAR || '|' ||
            fare_amount::VARCHAR
        )                                               AS trip_id,
        *
    FROM renamed
    WHERE
        trip_distance > 0
        AND fare_amount > 0
        AND total_amount > 0
        AND trip_duration_minutes > 0
        AND pickup_at < dropoff_at
)

SELECT * FROM final
