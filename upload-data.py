
#!/usr/bin/env python
# coding: utf-8
import sys
import os
import pandas as pd
from sqlalchemy import create_engine
import pyarrow as pa
from pyarrow.parquet import ParquetFile
from prefect import flow, task
# from prefect.tasks import task_input_hash
from datetime import timedelta
import argparse

@task(log_prints=True, retries=3)
def extract_data( url):
    parquet_name = "yellow_tripdata_2021-01.parquet"

    # download parquet
    os.system(f"wget {url} -O {parquet_name}")

    pf=ParquetFile(parquet_name)
    first_500k_rows = next(pf.iter_batches(batch_size = 200000)) 
    df = pa.Table.from_batches([first_500k_rows]).to_pandas() 

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime )
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime )

    return df

@task(log_prints=True)
def transform_data(df):
    print(f"PRE: MISSING PASSENGER COUNT: {df['passenger_count'].isin([0]).sum() }") 
    df = df[df['passenger_count'] != 0]
    print(f"POST: MISSING PASSENGER COUNT: {df['passenger_count'].isin([0]).sum() }") 
    return df

@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, data):
 
    df = data

    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(postgres_url)

    # engine.connect()

    print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    df.to_sql(name=table_name, con=engine, if_exists='replace')

    # query = """
    # select * from information_schema.tables  WHERE table_schema = 'public';
    # """ 
    # pd.read_sql(query, con=engine)

    # query = """
    # select * from yellow_taxi_trips;
    # """ 
    # pd.read_sql(query, con=engine)

@flow(name="Ingest Flow")
def main():
    user="root"
    password="root"
    host="localhost" 
    port="5432"
    db="ny_taxi"
    table_name="yellow_taxi_trips" 
    url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

    raw_data = extract_data(url)
    data = transform_data(raw_data)
    ingest_data(user, password, host, port, db, table_name, data)

if __name__ == '__main__':
    main()

    
