/*
  Model: int_trips_enriched
  Layer: Intermediate
  Depends on: stg_trips, stg_zones

  Purpose:
    - Denormalizes zone names onto trip records (pickup + dropoff zones).
    - Derives business-friendly categorical columns for analytics.
    - Calculates time-of-day and day-of-week segments for trend analysis.
    - This enriched view feeds directly into the final fact table.
*/

WITH trips AS (
    SELECT * FROM {{ ref('stg_trips') }}
),

zones AS (
    SELECT * FROM {{ ref('stg_zones') }}
),

trips_with_zones AS (
    SELECT
        t.*,

        -- Pickup zone enrichment
        pu_zone.borough                                 AS pickup_borough,
        pu_zone.zone                                    AS pickup_zone,
        pu_zone.service_zone                            AS pickup_service_zone,

        -- Dropoff zone enrichment
        do_zone.borough                                 AS dropoff_borough,
        do_zone.zone                                    AS dropoff_zone,
        do_zone.service_zone                            AS dropoff_service_zone

    FROM trips t
    LEFT JOIN zones pu_zone ON t.pickup_location_id = pu_zone.location_id
    LEFT JOIN zones do_zone ON t.dropoff_location_id = do_zone.location_id
),

enriched AS (
    SELECT
        *,

        -- Time-of-day segment (for peak hour analysis)
        CASE
            WHEN HOUR(pickup_at) BETWEEN 7  AND 9  THEN 'Morning Rush'
            WHEN HOUR(pickup_at) BETWEEN 10 AND 15 THEN 'Midday'
            WHEN HOUR(pickup_at) BETWEEN 16 AND 19 THEN 'Evening Rush'
            WHEN HOUR(pickup_at) BETWEEN 20 AND 23 THEN 'Night'
            ELSE 'Late Night / Early Morning'
        END                                             AS time_of_day_segment,

        -- Day of week
        DAYNAME(pickup_at)                              AS day_of_week,

        -- Weekend flag
        CASE
            WHEN DAYOFWEEK(pickup_at) IN (1, 7) THEN TRUE
            ELSE FALSE
        END                                             AS is_weekend,

        -- Trip distance bucket
        CASE
            WHEN trip_distance < 1   THEN 'Short (<1 mile)'
            WHEN trip_distance < 3   THEN 'Medium (1-3 miles)'
            WHEN trip_distance < 10  THEN 'Long (3-10 miles)'
            ELSE 'Extra Long (>10 miles)'
        END                                             AS distance_category,

        -- Payment type label
        CASE payment_type
            WHEN 1 THEN 'Credit Card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No Charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            WHEN 6 THEN 'Voided Trip'
            ELSE 'Other'
        END                                             AS payment_method

    FROM trips_with_zones
)

SELECT * FROM enriched
