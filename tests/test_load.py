"""
Unit & Integration Tests for Warehouse Loading Module (Level 3 - DuckDB)
"""

import pytest
import duckdb
from pathlib import Path
from src.load.load_to_duckdb import get_db_connection, load_raw_tables


def test_duckdb_connection_creation(tmp_path):
    """Verify DuckDB database file creation and connection."""
    db_file = tmp_path / "test_warehouse.duckdb"
    con = get_db_connection(str(db_file))
    try:
        assert db_file.exists()
        result = con.execute("SELECT 1 AS num;").fetchone()
        assert result[0] == 1
    finally:
        con.close()


def test_warehouse_schema_and_loading(tmp_path):
    """Verify that raw schema and tables can be initialized and queried."""
    db_file = tmp_path / "test_warehouse.duckdb"
    con = duckdb.connect(str(db_file))

    try:
        # Create a mock parquet file
        sample_parquet = tmp_path / "sample_trips.parquet"
        con.execute(f"""
            COPY (
                SELECT 
                    1 AS VendorID,
                    '2024-01-01 10:00:00'::TIMESTAMP AS tpep_pickup_datetime,
                    '2024-01-01 10:15:00'::TIMESTAMP AS tpep_dropoff_datetime,
                    2.5 AS trip_distance,
                    15.0 AS trip_duration_minutes,
                    20.0 AS fare_amount,
                    25.0 AS total_amount,
                    100 AS PULocationID,
                    101 AS DOLocationID
            ) TO '{sample_parquet.as_posix()}' (FORMAT PARQUET);
        """)

        # Create a mock zone csv
        sample_csv = tmp_path / "sample_zones.csv"
        sample_csv.write_text("LocationID,Borough,Zone,service_zone\n100,Manhattan,Times Sq,Yellow Zone\n")

        # Execute loader
        metrics = load_raw_tables(
            con=con,
            clean_parquet_path=str(sample_parquet),
            zone_csv_path=str(sample_csv),
        )

        assert metrics["trips_loaded"] == 1
        assert metrics["zones_loaded"] == 1
        assert metrics["total_revenue"] == 25.0
        assert metrics["avg_distance"] == 2.5
        assert metrics["avg_duration"] == 15.0

        # Verify tables in raw schema
        tables = [row[0] for row in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw';").fetchall()]
        assert "trips" in tables
        assert "zones" in tables
    finally:
        con.close()
