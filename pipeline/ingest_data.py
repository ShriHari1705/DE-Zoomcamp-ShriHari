#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# Data types and date columns specific to the NY Taxi dataset
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--user', default='root', help='PostgreSQL user name')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table_name', default='yellow_taxi_data', help='Target table name')
@click.option('--url', help='URL of the CSV file')
@click.option('--chunksize', default=100000, type=int, help='Size of data chunks for ingestion')
def run(user, password, host, port, db, table_name, url, chunksize):
    """Ingest NYC taxi data into a PostgreSQL database."""
    
    # Create the connection engine using the psycopg2 driver
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')

    # Initialize the CSV iterator
    df_iteration = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first = True

    # Ingest data in chunks
    for df_chunk in tqdm(df_iteration):
        if first:
            # Create the table schema by replacing any existing table of the same name
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists="replace"
            )
            first = False
            print(f"Table '{table_name}' created or replaced.")

        # Append the chunk to the table
        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append"
        )

        print(f"Inserted chunk with {len(df_chunk)} rows.")

if __name__ == '__main__':
    run()