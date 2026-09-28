{{
    config(
        materialized='view',
        description='Staging layer: clean and rename columns from the raw taxi zone reference table.'
    )
}}

/*
  stg_zones — Staging Layer
  -------------------------
  Reads from raw.zones (loaded by the DuckDB bulk loader from taxi_zone_lookup.csv).
  Responsibilities:
    1. Rename columns to snake_case analytical standard.
    2. Enforce strict type casting.
*/

SELECT
    CAST(location_id AS INTEGER)     AS location_id,
    CAST(borough AS VARCHAR)         AS borough,
    CAST(zone AS VARCHAR)            AS zone_name,
    CAST(service_zone AS VARCHAR)    AS service_zone

FROM {{ source('raw', 'zones') }}
