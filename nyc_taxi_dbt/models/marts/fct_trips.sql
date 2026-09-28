{{
    config(
        materialized='table',
        description='Mart: Fact table for individual taxi trips — the primary analytical table for all trip-level metrics.'
    )
}}

/*
  fct_trips — Fact Table (Star Schema Mart)
  ------------------------------------------
  Production-certified trip records with all business metrics and foreign keys
  pointing to dimension tables (dim_zones, dim_date).

  Grain: One row per individual taxi trip.
  Primary Key: trip_id
*/

SELECT
    -- Primary key
    trip_id,

    -- Foreign keys → dimension tables
    pickup_location_id,
    dropoff_location_id,
    pickup_date,

    -- Degenerate dimensions (captured at transaction time, no dim table needed)
    vendor_id,
    time_of_day_segment,
    payment_method,
    is_weekend,
    passenger_count,

    -- Zone names (denormalized for query performance)
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,

    -- Timestamps
    pickup_at,
    dropoff_at,

    -- Additive measures
    trip_distance_miles,
    trip_duration_minutes,
    avg_speed_mph,
    fare_amount,
    tip_amount,
    tip_percentage,
    tolls_amount,
    congestion_surcharge,
    airport_fee,
    total_amount

FROM {{ ref('int_trips_enriched') }}
