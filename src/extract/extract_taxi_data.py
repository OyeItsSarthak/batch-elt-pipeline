"""
Data Extraction Module (Level 1)
--------------------------------
Downloads raw NYC Taxi & Limousine Commission (TLC) trip data and taxi zone
lookup tables into a local or cloud landing zone.

Key Engineering Concepts:
1. Landing Zone: Storing untouched raw data for auditability and idempotency.
2. Streaming I/O: Streaming chunked HTTP downloads to prevent memory spikes.
3. Idempotent Execution: Skipping downloads if valid file already exists.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("extract_taxi_data")

NYC_TLC_BASE_URL = os.getenv(
    "NYC_TLC_BASE_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data"
)
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def download_file(url: str, target_path: Path, chunk_size: int = 1024 * 1024) -> Path:
    """
    Streams a remote file to a destination path with idempotency checks.

    Args:
        url: Remote endpoint to fetch.
        target_path: Local path where the file should be saved.
        chunk_size: Stream buffer size in bytes (default 1MB).

    Returns:
        Path to the downloaded file.
    """
    target_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if file already exists and has non-zero size
    if target_path.exists() and target_path.stat().st_size > 0:
        logger.info(f"File already landed at {target_path} ({target_path.stat().st_size / (1024*1024):.2f} MB). Skipping download.")
        return target_path

    logger.info(f"Streaming download from: {url}")
    with requests.get(url, stream=True, timeout=60) as response:
        if response.status_code == 404:
            raise FileNotFoundError(f"Requested dataset not found at {url} (HTTP 404)")
        response.raise_for_status()

        total_bytes = int(response.headers.get("content-length", 0))
        downloaded_bytes = 0

        with open(target_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded_bytes += len(chunk)
                    if total_bytes > 0:
                        progress = (downloaded_bytes / total_bytes) * 100
                        print(f"\rDownloading: {progress:.1f}% ({downloaded_bytes / (1024*1024):.2f} MB)", end="", flush=True)

    print()  # Newline after progress bar
    logger.info(f"Successfully landed: {target_path} ({target_path.stat().st_size / (1024*1024):.2f} MB)")
    return target_path


def extract_monthly_trips(
    year: int,
    month: int,
    taxi_type: str = "yellow",
    output_dir: str = "data/raw",
) -> Path:
    """
    Downloads monthly NYC TLC trip data in Parquet format.
    """
    formatted_month = f"{month:02d}"
    filename = f"{taxi_type}_tripdata_{year}-{formatted_month}.parquet"
    url = f"{NYC_TLC_BASE_URL}/{filename}"
    target_path = Path(output_dir) / filename

    logger.info(f"Initiating extraction for {taxi_type} taxi ({year}-{formatted_month})...")
    return download_file(url=url, target_path=target_path)


def extract_zone_lookup(output_dir: str = "data/raw") -> Path:
    """
    Downloads the NYC TLC Taxi Zone Lookup reference CSV.
    """
    target_path = Path(output_dir) / "taxi_zone_lookup.csv"
    logger.info("Initiating extraction for taxi zone reference table...")
    return download_file(url=ZONE_LOOKUP_URL, target_path=target_path)


def main():
    parser = argparse.ArgumentParser(description="NYC TLC Trip Data Extractor")
    parser.add_argument("--year", type=int, default=2024, help="Year of trip data (e.g. 2024)")
    parser.add_argument("--month", type=int, default=1, help="Month of trip data (1-12)")
    parser.add_argument("--taxi-type", type=str, default="yellow", choices=["yellow", "green"], help="Taxi fleet type")
    parser.add_argument("--output-dir", type=str, default="data/raw", help="Target landing zone directory")
    parser.add_argument("--include-zones", action="store_true", help="Also download taxi zone lookup CSV")

    args = parser.parse_args()

    # Land monthly trip parquet
    extract_monthly_trips(
        year=args.year,
        month=args.month,
        taxi_type=args.taxi_type,
        output_dir=args.output_dir,
    )

    # Land zone reference data
    if args.include_zones:
        extract_zone_lookup(output_dir=args.output_dir)


if __name__ == "__main__":
    main()
