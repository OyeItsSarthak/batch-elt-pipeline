{% docs stg_trips %}
Staged trip records from `raw.trips`. Column names are snake_case, types are explicit,
a surrogate `trip_id` is hashed, and safety filters drop remaining invalid rows.
This is the only trip model downstream models should depend on.
{% enddocs %}

{% docs stg_zones %}
Staged TLC taxi zone lookup from `raw.zones`. Grain is one row per `location_id`.
{% enddocs %}

{% docs int_trips_enriched %}
Trips joined to pickup and dropoff zones, plus categorical attributes used by the fact table
(time-of-day, weekend flag, distance bucket, payment method).
{% enddocs %}

{% docs fct_trips %}
Fact table: one row per completed yellow taxi trip. Contains measures (distance, duration,
fares) and foreign keys to `dim_zones` and `dim_date`. Zone names live on the dimension,
not on this table — that is the star schema.
{% enddocs %}

{% docs dim_zones %}
Zone dimension: borough, zone name, and service zone. Join to `fct_trips` twice
(pickup and dropoff) on `location_id`.
{% enddocs %}

{% docs dim_date %}
Date dimension spanning `trip_start_date` (inclusive) to `trip_end_date` (exclusive).
Join to `fct_trips.pickup_date`.
{% enddocs %}
