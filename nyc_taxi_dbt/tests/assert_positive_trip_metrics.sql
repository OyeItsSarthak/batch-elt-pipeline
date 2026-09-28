-- Measures used in reporting must stay strictly positive after staging.
-- NOTE: trip_distance was renamed to trip_distance_miles in the staging layer.
select
    trip_id,
    trip_distance_miles,
    fare_amount,
    total_amount,
    trip_duration_minutes
from {{ ref('stg_trips') }}
where trip_distance_miles <= 0
   or fare_amount <= 0
   or total_amount <= 0
   or trip_duration_minutes <= 0
