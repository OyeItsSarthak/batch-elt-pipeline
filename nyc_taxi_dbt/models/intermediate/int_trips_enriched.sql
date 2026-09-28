{{
    config(
        materialized='table',
        description='Intermediate layer: enrich standardized trips with zone names, time segments, and payment labels.'
    )
}}

/*
  int_trips_enriched — Intermediate Layer
  ----------------------------------------
  Joins stg_trips with stg_zones to denormalize pickup/dropoff borough and zone names.
  Applies business logic macros:
    - time_of_day_segment: classifies trips by time of day
    - payment_method: decodes payment_type_code to human-readable label
    - is_weekend: flags weekend trips
  This is the analytical source of truth consumed by mart models.
*/

WITH trips AS (
    SELECT * FROM {{ ref('stg_trips') }}
),

pickup_zones AS (
    SELECT
        location_id,
        borough    AS pickup_borough,
        zone_name  AS pickup_zone
    FROM {{ ref('stg_zones') }}
),

dropoff_zones AS (
    SELECT
        location_id,
        borough    AS dropoff_borough,
        zone_name  AS dropoff_zone
    FROM {{ ref('stg_zones') }}
)

SELECT
    -- Keys
    t.trip_id,
    t.vendor_id,
    t.pickup_location_id,
    t.dropoff_location_id,

    -- Timestamps
    t.pickup_at,
    t.dropoff_at,
    DATE_TRUNC('day', t.pickup_at)::DATE  AS pickup_date,

    -- Zone enrichment (LEFT JOIN: preserve trips even if zone lookup is incomplete)
    pu.pickup_borough,
    pu.pickup_zone,
    do_.dropoff_borough,
    do_.dropoff_zone,

    -- Business logic via macros
    {{ time_of_day_segment('t.pickup_at') }}  AS time_of_day_segment,
    {{ payment_method('t.payment_type_code') }} AS payment_method,
    {{ is_weekend('t.pickup_at') }}            AS is_weekend,

    -- Trip metrics
    t.passenger_count,
    t.trip_distance_miles,
    t.trip_duration_minutes,
    t.avg_speed_mph,

    -- Financials
    t.fare_amount,
    t.tip_amount,
    t.tip_percentage,
    t.tolls_amount,
    t.congestion_surcharge,
    t.airport_fee,
    t.total_amount

FROM trips t
LEFT JOIN pickup_zones  pu   ON t.pickup_location_id  = pu.location_id
LEFT JOIN dropoff_zones do_  ON t.dropoff_location_id = do_.location_id
