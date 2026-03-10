"""@bruin

name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

depends:
  - ingestion.payment_lookup

columns:
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)
  - name: vendor_id
    type: BIGINT
    description: Vendor identifier
  - name: pickup_datetime
    type: TIMESTAMP
    description: Trip pickup datetime
  - name: dropoff_datetime
    type: TIMESTAMP
    description: Trip dropoff datetime
  - name: passenger_count
    type: DOUBLE
    description: Number of passengers
  - name: trip_distance
    type: DOUBLE
    description: Trip distance in miles
  - name: fare_amount
    type: DOUBLE
    description: Fare amount in USD
  - name: total_amount
    type: DOUBLE
    description: Total amount charged
  - name: payment_type
    type: BIGINT
    description: Payment type code
  - name: pu_location_id
    type: BIGINT
    description: Pickup location ID
  - name: do_location_id
    type: BIGINT
    description: Dropoff location ID
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the record was extracted

@bruin"""

import json
import os
from pathlib import Path

import duckdb
import pandas as pd
import yaml


def get_duckdb_path():
    """Read the DuckDB file path from the nearest .bruin.yml file."""
    conn_name = os.environ.get("BRUIN_CONNECTION", "duckdb-default")
    # Walk up from script dir to find .bruin.yml
    current = Path(__file__).resolve().parent
    for _ in range(10):
        candidate = current / ".bruin.yml"
        if candidate.exists():
            with open(candidate) as f:
                config = yaml.safe_load(f)
            env = config.get("default_environment", "default")
            duckdb_conns = (
                config.get("environments", {})
                .get(env, {})
                .get("connections", {})
                .get("duckdb", [])
            )
            for conn in duckdb_conns:
                if conn["name"] == conn_name:
                    return conn["path"]
        parent = current.parent
        if parent == current:
            break
        current = parent
    return "/tmp/taxi.db"


def main():
    start_date = os.environ.get("BRUIN_START_DATE")
    end_date = os.environ.get("BRUIN_END_DATE")
    db_path = get_duckdb_path()
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow", "green"])

    print(f"Using DuckDB at: {db_path}", flush=True)
    months = pd.date_range(start=start_date, end=end_date, freq="MS")
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS ingestion")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestion.trips (
            taxi_type VARCHAR,
            vendor_id BIGINT,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            total_amount DOUBLE,
            payment_type BIGINT,
            pu_location_id BIGINT,
            do_location_id BIGINT,
            extracted_at TIMESTAMP
        )
    """)

    col_map = {
        "green":  {"pickup": "lpep_pickup_datetime",  "dropoff": "lpep_dropoff_datetime"},
        "yellow": {"pickup": "tpep_pickup_datetime",  "dropoff": "tpep_dropoff_datetime"},
    }

    for taxi_type in taxi_types:
        for month in months:
            year = month.strftime("%Y")
            month_str = month.strftime("%m")
            url = f"{base_url}/{taxi_type}_tripdata_{year}-{month_str}.parquet"
            try:
                pickup_col  = col_map[taxi_type]["pickup"]
                dropoff_col = col_map[taxi_type]["dropoff"]
                row_count = con.execute(f"""
                    INSERT INTO ingestion.trips
                    SELECT
                        '{taxi_type}'             AS taxi_type,
                        TRY_CAST(VendorID AS BIGINT),
                        TRY_CAST("{pickup_col}"  AS TIMESTAMP),
                        TRY_CAST("{dropoff_col}" AS TIMESTAMP),
                        TRY_CAST(passenger_count AS DOUBLE),
                        TRY_CAST(trip_distance   AS DOUBLE),
                        TRY_CAST(fare_amount     AS DOUBLE),
                        TRY_CAST(total_amount    AS DOUBLE),
                        TRY_CAST(payment_type    AS BIGINT),
                        TRY_CAST(PULocationID    AS BIGINT),
                        TRY_CAST(DOLocationID    AS BIGINT),
                        CURRENT_TIMESTAMP        AS extracted_at
                    FROM read_parquet('{url}')
                """).fetchone()
                total = con.execute("SELECT COUNT(*) FROM ingestion.trips WHERE taxi_type = ? AND TRY_CAST(pickup_datetime AS DATE) >= ?", [taxi_type, f"{year}-{month_str}-01"]).fetchone()[0]
                print(f"Inserted rows for {taxi_type} {year}-{month_str}", flush=True)
            except Exception as e:
                print(f"Skipping {taxi_type} {year}-{month_str}: {e}", flush=True)

    con.close()


main()
