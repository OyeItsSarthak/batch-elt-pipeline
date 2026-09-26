/*
  Model: fct_trips
  Layer: Marts (Fact Table)
  Depends on: int_trips_enriched
  Materialized: TABLE (pre-computed for analytical query performance)

  Purpose:
    - Production-ready fact table exposing one row per completed trip.
    - Contains all trip metrics, time dimensions, zone keys, and financial KPIs.
    - Designed for direct querying by BI tools (e.g., Streamlit, Metabase, Tableau).

  Star Schema Role: Central FACT TABLE in the Star Schema.
*/

WITH enriched AS (
    SELECT * FROM {{ ref('int_trips_enriched') }}
)

SELECT
    -- === Primary Key ===
    trip_id,

    -- === Foreign Keys (links to dimension tables) ===
    pickup_location_id,
    dropoff_location_id,

    -- === Degenerate Dimensions (no separate dimension table needed) ===
    vendor_id,
    rate_code_id,
    payment_type,
    payment_method,
    store_and_fwd_flag,

    -- === Date / Time Dimensions ===
    pickup_at,
    dropoff_at,
    DATE(pickup_at)                                     AS pickup_date,
    YEAR(pickup_at)                                     AS pickup_year,
    MONTH(pickup_at)                                    AS pickup_month,
    DAYOFMONTH(pickup_at)                               AS pickup_day,
    HOUR(pickup_at)                                     AS pickup_hour,
    day_of_week,
    is_weekend,
    time_of_day_segment,

    -- === Zone Labels (denormalized for query convenience) ===
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone,

    -- === Trip Metrics ===
    passenger_count,
    trip_distance,
    distance_category,
    trip_duration_minutes,
    avg_speed_mph,

    -- === Financial Measures ===
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tip_percentage,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount

FROM enriched
