{{ config(materialized='table', alias='dim_date') }}

/*
  dim_date — Date Dimension Table
  --------------------------------
  Generates a calendar dimension spanning all dates in the loaded dataset.
  Uses DuckDB's native GENERATE_SERIES — no dbt_utils dependency needed.

  Grain: One row per calendar date.
  Primary Key: date_day
*/

WITH date_spine AS (
    SELECT UNNEST(
        GENERATE_SERIES(
            '2024-01-01'::DATE,
            '2024-12-31'::DATE,
            INTERVAL 1 DAY
        )
    )::DATE AS date_day
)

SELECT
    date_day,
    EXTRACT(year FROM date_day)::INTEGER          AS year,
    EXTRACT(month FROM date_day)::INTEGER         AS month,
    EXTRACT(day FROM date_day)::INTEGER           AS day_of_month,
    EXTRACT(quarter FROM date_day)::INTEGER       AS quarter,
    STRFTIME(date_day, '%A')                      AS day_of_week,
    {{ is_weekend('date_day') }}                   AS is_weekend,
    EXTRACT(week FROM date_day)::INTEGER          AS iso_week

FROM date_spine
