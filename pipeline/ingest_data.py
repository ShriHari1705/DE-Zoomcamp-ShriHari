import pandas as pd
from sqlalchemy import create_engine
from time import time
import click

@click.command()
@click.option('--user', help='username for postgres')
@click.option('--password', help='password for postgres')
@click.option('--host', help='host for postgres')
@click.option('--port', help='port for postgres')
@click.option('--db', help='database name for postgres')
@click.option('--table_name', help='name of the table where we will write the results to')
@click.option('--url', help='url of the csv file')
def run(user, password, host, port, db, table_name, url):
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # PANDAS READS THE URL DIRECTLY
    # It handles .csv or .csv.gz automatically
    df_iter = pd.read_csv(url, iterator=True, chunksize=100000)

    try:
        df = next(df_iter)
    except Exception as e:
        print(f"Error reading the URL: {e}")
        return

    # Check for date columns (Flexible logic)
    if 'tpep_pickup_datetime' in df.columns:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Create table and insert first chunk
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()
            df = next(df_iter)

            if 'tpep_pickup_datetime' in df.columns:
                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()
            print(f'inserted another chunk..., took {t_end - t_start:.3f} seconds')
        except StopIteration:
            print("Finished ingesting data.")
            break

if __name__ == '__main__':
    run()