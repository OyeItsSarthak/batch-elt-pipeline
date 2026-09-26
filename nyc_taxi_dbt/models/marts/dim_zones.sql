/*
  Model: dim_zones
  Layer: Marts (Dimension Table)
  Depends on: stg_zones
  Materialized: TABLE

  Purpose:
    - Production-ready zone dimension table with 265 NYC Taxi zones.
    - Joins to fct_trips via pickup_location_id / dropoff_location_id.
    - Provides human-readable borough and zone names for BI tools.

  Star Schema Role: DIMENSION TABLE in the Star Schema.
*/

WITH zones AS (
    SELECT * FROM {{ ref('stg_zones') }}
)

SELECT
    location_id,
    borough,
    zone,
    service_zone,

    -- Borough-level grouping for regional analytics
    CASE borough
        WHEN 'Manhattan'    THEN 1
        WHEN 'Queens'       THEN 2
        WHEN 'Brooklyn'     THEN 3
        WHEN 'Bronx'        THEN 4
        WHEN 'Staten Island' THEN 5
        ELSE 6
    END                                                 AS borough_sort_order

FROM zones
ORDER BY location_id
