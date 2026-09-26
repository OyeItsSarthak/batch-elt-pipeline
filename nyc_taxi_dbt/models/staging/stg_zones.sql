/*
  Model: stg_zones
  Layer: Staging
  Source: raw.zones (loaded by DuckDB bulk loader)

  Purpose:
    - Standardizes raw zone column names to snake_case.
    - Represents the NYC TLC Taxi Zone dimension reference table.
*/

WITH source AS (
    SELECT * FROM raw.zones
),

renamed AS (
    SELECT
        location_id,
        borough,
        zone,
        service_zone
    FROM source
    WHERE location_id IS NOT NULL
)

SELECT * FROM renamed
