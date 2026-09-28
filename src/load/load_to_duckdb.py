"""
Analytical Warehouse Loading Module (Level 3 - DuckDB)
------------------------------------------------------
Loads cleaned TLC trip data and reference lookup tables into DuckDB,
a high-performance, columnar vectorized analytical OLAP database.

Key Data Engineering Concepts:
1. Columnar OLAP: Vectorized execution engine optimized for analytical aggregates.
2. In-Database Schemas: Partitioning datasets into logical schemas (e.g., 'raw').
3. Zero-Copy Ingestion: Leveraging direct Parquet and CSV readers without memory thrashing.
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("load_to_duckdb")

DEFAULT_DB_PATH = os.getenv("DUCKDB_DATABASE_PATH", "data/warehouse/nyc_taxi.duckdb")


def get_db_connection(db_path: str = DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection:
    """
    Establishes connection to the DuckDB analytical database file.
    Creates parent directories if necessary.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Connecting to DuckDB database at: {db_path}")
    return duckdb.connect(db_path)


def load_raw_tables(
    con: duckdb.DuckDBPyConnection,
    clean_parquet_path: str,
    zone_csv_path: str = "data/raw/taxi_zone_lookup.csv",
) -> dict[str, Any]:
    """
    Loads raw trip data and taxi zones into the 'raw' warehouse schema.

    Args:
        con: Active DuckDB connection.
        clean_parquet_path: Path to PySpark cleaned parquet dataset.
        zone_csv_path: Path to raw taxi zone lookup CSV.

    Returns:
        Summary metrics dictionary of the load.
    """
    # 1. Initialize schema
    logger.info("Initializing 'raw' schema in DuckDB warehouse...")
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    # 2. Bulk load cleaned trips
    # Support both single parquet file or spark parquet directory (with part-*.parquet)
    parquet_target = clean_parquet_path
    if Path(clean_parquet_path).is_dir():
        parquet_target = f"{clean_parquet_path}/*.parquet"

    logger.info(f"Bulk-loading trips into 'raw.trips' from: {parquet_target}")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.trips AS
        SELECT * FROM read_parquet('{parquet_target}');
    """)

    trip_count = con.execute("SELECT COUNT(*) FROM raw.trips;").fetchone()[0]
    logger.info(f"Successfully loaded 'raw.trips': {trip_count:,} records.")

    # 3. Bulk load reference zones if available
    zone_count = 0
    if Path(zone_csv_path).exists():
        logger.info(f"Loading reference zone lookup from: {zone_csv_path}")
        con.execute(f"""
            CREATE OR REPLACE TABLE raw.zones AS
            SELECT
                LocationID::INTEGER AS location_id,
                Borough AS borough,
                Zone AS zone,
                service_zone
            FROM read_csv_auto('{zone_csv_path}');
        """)
        zone_count = con.execute("SELECT COUNT(*) FROM raw.zones;").fetchone()[0]
        logger.info(f"Successfully loaded 'raw.zones': {zone_count:,} records.")

    # 4. Warehouse Validation Metrics
    kpi_row = con.execute("""
        SELECT
            COUNT(*) AS total_trips,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
            ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_mins
        FROM raw.trips;
    """).fetchone()

    metrics = {
        "trips_loaded": trip_count,
        "zones_loaded": zone_count,
        "total_revenue": kpi_row[1],
        "avg_distance": kpi_row[2],
        "avg_duration": kpi_row[3],
    }

    logger.info("=== Warehouse Summary Metrics ===")
    logger.info(f"Total Trips:    {metrics['trips_loaded']:,}")
    logger.info(f"Total Revenue:  ${metrics['total_revenue']:,}")
    logger.info(f"Avg Distance:   {metrics['avg_distance']} miles")
    logger.info(f"Avg Duration:   {metrics['avg_duration']} minutes")

    return metrics


def main():
    parser = argparse.ArgumentParser(description="DuckDB Data Warehouse Loader")
    parser.add_argument(
        "--input-parquet",
        type=str,
        default="data/processed/yellow_tripdata_2024-01_clean.parquet",
        help="Path to clean parquet file or Spark directory",
    )
    parser.add_argument(
        "--input-zones",
        type=str,
        default="data/raw/taxi_zone_lookup.csv",
        help="Path to zone lookup CSV",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=DEFAULT_DB_PATH,
        help="Target DuckDB database path",
    )

    args = parser.parse_args()

    con = get_db_connection(db_path=args.db_path)
    try:
        load_raw_tables(
            con=con,
            clean_parquet_path=args.input_parquet,
            zone_csv_path=args.input_zones,
        )
    finally:
        con.close()
        logger.info("Warehouse connection safely closed.")


if __name__ == "__main__":
    main()
