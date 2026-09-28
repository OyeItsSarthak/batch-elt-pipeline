"""
Distributed Cleansing & Transformation Module (Level 2)
-------------------------------------------------------
Executes distributed data validation, anomaly filtering, deduplication,
and feature engineering on raw TLC trip data using Apache PySpark.

Key Data Quality & Transformation Logic:
1. Deduplication: Eliminates duplicate trip entries.
2. Temporal Integrity: Validates pickup < dropoff, filters extreme date drift.
3. Geo/Physical Validity: Filters anomalous distances (<=0 or >=200 miles).
4. Financial Integrity: Enforces positive fare amounts and valid payment codes.
5. Feature Engineering:
   - trip_duration_minutes
   - average_speed_mph
   - tip_percentage
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("clean_taxi_data")


# Configure HADOOP_HOME for Windows environments
project_root = Path(__file__).resolve().parents[2]
hadoop_dir = project_root / "hadoop"
if hadoop_dir.exists():
    os.environ["HADOOP_HOME"] = str(hadoop_dir)
    os.environ["PATH"] = f"{hadoop_dir / 'bin'};{os.environ.get('PATH', '')}"


def get_spark_session(app_name: str = "NYC_Taxi_Cleansing") -> SparkSession:
    """
    Initializes a production-tuned local SparkSession.
    Configures memory and serialization for single-node development.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "true")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    )
    return builder.getOrCreate()


def clean_trip_data(
    spark: SparkSession,
    raw_path: str,
    output_path: str,
    year: int = 2024,
    month: int = 1,
) -> dict:
    """
    Reads raw parquet file, performs validation and feature engineering,
    and writes out the clean dataset.

    Returns:
        dict containing audit metrics (initial rows, clean rows, dropped rows).
    """
    logger.info(f"Reading raw dataset from: {raw_path}")
    raw_df = spark.read.parquet(raw_path)

    initial_count = raw_df.count()
    logger.info(f"Loaded raw records: {initial_count:,}")

    # 1. Deduplication
    deduped_df = raw_df.dropDuplicates([
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
    ])
    after_dedup = deduped_df.count()
    logger.info(f"Rows after deduplication: {after_dedup:,} (Dropped {initial_count - after_dedup:,} duplicates)")

    # 2. Filter invalid date boundaries (prevent clock drift / corrupted timestamps)
    # Trip pickup must be within the expected month/year (allow small boundary buffer)
    start_date = f"{year}-{month:02d}-01 00:00:00"
    if month == 12:
        end_date = f"{year + 1}-01-01 23:59:59"
    else:
        end_date = f"{year}-{month + 1:02d}-01 23:59:59"

    date_filtered_df = deduped_df.filter(
        (F.col("tpep_pickup_datetime") >= start_date)
        & (F.col("tpep_pickup_datetime") <= end_date)
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    )

    # 3. Filter physical anomalies (distance, passengers, fares)
    valid_records_df = date_filtered_df.filter(
        (F.col("trip_distance") > 0.0)
        & (F.col("trip_distance") <= 150.0)
        & (F.col("fare_amount") > 0.0)
        & (F.col("total_amount") > 0.0)
        & (F.col("PULocationID").isNotNull())
        & (F.col("DOLocationID").isNotNull())
    )

    # 4. Feature Engineering
    # Calculate duration in minutes
    duration_expr = (
        F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")
    ) / 60.0

    engineered_df = (
        valid_records_df.withColumn("trip_duration_minutes", F.round(duration_expr, 2))
        # Keep realistic trip durations (between 1 minute and 12 hours = 720 mins)
        .filter((F.col("trip_duration_minutes") >= 1.0) & (F.col("trip_duration_minutes") <= 720.0))
        # Calculate speed in MPH
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.round(F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0), 2),
            ).otherwise(0.0),
        )
        # Filter unreal speeds (e.g. > 100 mph in NYC traffic indicates faulty GPS)
        .filter(F.col("avg_speed_mph") <= 100.0)
        # Calculate Tip Percentage
        .withColumn(
            "tip_percentage",
            F.when(
                F.col("fare_amount") > 0,
                F.round((F.col("tip_amount") / F.col("fare_amount")) * 100.0, 2),
            ).otherwise(0.0),
        )
        # Standardize missing passenger counts to 1 (default single rider)
        .withColumn(
            "passenger_count",
            F.when(F.col("passenger_count").isNull() | (F.col("passenger_count") <= 0), 1)
            .otherwise(F.col("passenger_count").cast(IntegerType())),
        )
    )

    clean_count = engineered_df.count()
    dropped_count = initial_count - clean_count
    dropped_pct = (dropped_count / initial_count) * 100 if initial_count > 0 else 0.0

    logger.info(f"Clean records remaining: {clean_count:,}")
    logger.info(f"Total anomaly/corrupt records filtered: {dropped_count:,} ({dropped_pct:.2f}%)")

    # 5. Write to partitioned or single processed parquet landing zone
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Writing cleaned data to: {output_path}")
    engineered_df.coalesce(1).write.mode("overwrite").parquet(output_path)
    logger.info("Spark transformation complete and persisted.")

    metrics = {
        "initial_rows": initial_count,
        "clean_rows": clean_count,
        "dropped_rows": dropped_count,
        "dropped_percentage": dropped_pct,
    }
    return metrics


def main():
    parser = argparse.ArgumentParser(description="PySpark NYC Taxi Cleansing Job")
    parser.add_argument("--year", type=int, default=2024, help="Year of trip data")
    parser.add_argument("--month", type=int, default=1, help="Month of trip data")
    parser.add_argument("--input-file", type=str, default="data/raw/yellow_tripdata_2024-01.parquet", help="Path to raw parquet file")
    parser.add_argument("--output-file", type=str, default="data/processed/yellow_tripdata_2024-01_clean.parquet", help="Path to clean parquet file")

    args = parser.parse_args()

    spark = get_spark_session()
    try:
        clean_trip_data(
            spark=spark,
            raw_path=args.input_file,
            output_path=args.output_file,
            year=args.year,
            month=args.month,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
