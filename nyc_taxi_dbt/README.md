# nyc_taxi_dbt

Dimensional models on top of the warehouse `raw` schema. Staging standardizes names and types, intermediate joins zones and derives categories, marts are a star schema for analysts.

```text
raw.trips / raw.zones
        │
        ▼
  stg_trips / stg_zones     (views, snake_case, tests)
        │
        ▼
  int_trips_enriched        (view, zone names + time buckets)
        │
        ├── dim_zones
        ├── dim_date
        └── fct_trips       (tables — fact + dimensions)
```

**Grain**
- `fct_trips`: one row per completed trip (`trip_id`)
- `dim_zones`: one row per TLC location (`location_id`)
- `dim_date`: one row per calendar day in `trip_start_date` … `trip_end_date`

Zone names are **not** stored on the fact table. Join `dim_zones` twice (pickup and dropoff). That is the star schema.

## Run

From the repo root, with the virtualenv active and `data/warehouse/nyc_taxi.duckdb` already loaded:

```bash
cd nyc_taxi_dbt
dbt deps --profiles-dir .
dbt debug --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

Override the database file with `DUCKDB_DATABASE_PATH` if needed. `profiles.yml.example` is the same DuckDB config for copying into `~/.dbt` if you prefer not to pass `--profiles-dir`.

## Tests

Generic tests live next to models in `schema.yml` (unique, not_null, accepted_values, relationships, accepted_range). Two singular tests under `tests/` catch pickup-after-dropoff and non-positive measures. Marts `fct_trips` and `dim_zones` also enforce a dbt **contract** (column names and types).

## Dates

`dim_date` is built with `dbt_utils.date_spine`. Change the range in `dbt_project.yml`:

```yaml
vars:
  trip_start_date: "2024-01-01"
  trip_end_date: "2025-01-01"   # exclusive
```
