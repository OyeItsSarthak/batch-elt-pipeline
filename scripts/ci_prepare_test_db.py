"""
CI Test Database Initializer
----------------------------
Seeds a lightweight, schema-compliant synthetic DuckDB database for CI/CD runs.
Enables fast, end-to-end execution of dbt run and dbt test (40+ data quality contracts)
in GitHub Actions without downloading multi-gigabyte raw files.
"""

import os
from pathlib import Path

import duckdb

DB_PATH = os.getenv("DUCKDB_DATABASE_PATH", "data/warehouse/nyc_taxi.duckdb")


def seed_ci_database(db_path: str = DB_PATH):
    target = Path(db_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()

    print(f"Initializing synthetic DuckDB warehouse at: {target.resolve()}")
    con = duckdb.connect(str(target))

    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        # 1. Populate raw.zones (Full 265 standard taxi zones or mock zones)
        con.execute("""
            CREATE TABLE raw.zones AS
            SELECT
                range AS location_id,
                CASE
                    WHEN range % 4 = 0 THEN 'Manhattan'
                    WHEN range % 4 = 1 THEN 'Queens'
                    WHEN range % 4 = 2 THEN 'Brooklyn'
                    ELSE 'Bronx'
                END AS borough,
                'Zone ' || range AS zone,
                'Yellow Zone' AS service_zone
            FROM range(1, 266);
        """)
        zone_count = con.execute("SELECT COUNT(*) FROM raw.zones;").fetchone()[0]
        print(f"Created raw.zones with {zone_count} reference zones.")

        # 2. Populate raw.trips (Realistic synthetic records)
        con.execute("""
            CREATE TABLE raw.trips AS
            SELECT
                (1 + (range % 2))::INTEGER AS VendorID,
                ('2024-01-15 08:00:00'::TIMESTAMP + INTERVAL (range * 10) MINUTE) AS tpep_pickup_datetime,
                ('2024-01-15 08:20:00'::TIMESTAMP + INTERVAL (range * 10) MINUTE) AS tpep_dropoff_datetime,
                (1 + (range % 4))::INTEGER AS passenger_count,
                (1.5 + (range % 10) * 0.8)::DOUBLE AS trip_distance,
                1::INTEGER AS RatecodeID,
                'N' AS store_and_fwd_flag,
                (1 + (range % 250))::INTEGER AS PULocationID,
                (2 + (range % 250))::INTEGER AS DOLocationID,
                (1 + (range % 2))::INTEGER AS payment_type,
                (12.0 + (range % 15))::DOUBLE AS fare_amount,
                1.0::DOUBLE AS extra,
                0.5::DOUBLE AS mta_tax,
                (2.0 + (range % 5))::DOUBLE AS tip_amount,
                0.0::DOUBLE AS tolls_amount,
                1.0::DOUBLE AS improvement_surcharge,
                (19.0 + (range % 20))::DOUBLE AS total_amount,
                2.5::DOUBLE AS congestion_surcharge,
                0.0::DOUBLE AS Airport_fee,
                20.0::DOUBLE AS trip_duration_minutes,
                12.5::DOUBLE AS avg_speed_mph,
                16.67::DOUBLE AS tip_percentage
            FROM range(1, 101);
        """)
        trip_count = con.execute("SELECT COUNT(*) FROM raw.trips;").fetchone()[0]
        print(f"Created raw.trips with {trip_count} test trips.")

    finally:
        con.close()
    print("CI test database successfully prepared.")


if __name__ == "__main__":
    seed_ci_database()
