-- Grain check: pickup must always be strictly before dropoff.
select
    trip_id,
    pickup_at,
    dropoff_at
from {{ ref('stg_trips') }}
where pickup_at >= dropoff_at
