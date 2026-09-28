{{
    config(
        materialized='table',
        description='Dimension table for NYC TLC taxi zones — maps location IDs to boroughs and zone names.'
    )
}}

/*
  dim_zones — Zone Dimension Table (Star Schema)
  -----------------------------------------------
  Conformed dimension table for all 265 NYC TLC taxi zones.
  Provides human-readable borough and zone name lookups for fact table joins.

  Grain: One row per unique TLC location ID.
  Primary Key: location_id
*/

SELECT
    location_id,
    borough,
    zone_name,
    service_zone

FROM {{ ref('stg_zones') }}
