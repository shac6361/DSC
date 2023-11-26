import argparse
import pandas as pd
import numpy as np
import sqlalchemy
import os
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
import pyarrow.parquet as pq
from airflow.hooks.base_hook import BaseHook
from google.cloud.storage import Client
from google.cloud import storage
import sys
from io import BytesIO
from google.oauth2 import service_account
from datetime import datetime


def upload_df_to_gcs(dataframe, bucket_name, remote_blob_name, credentials):
    
    current_date_str = datetime.now().strftime("%Y%m%d")
    remote_blob_name_ = f"{current_date_str}/{remote_blob_name}"
    parquet_buffer = BytesIO()
    
    dataframe.to_parquet(parquet_buffer, engine='pyarrow')
    parquet_buffer.seek(0)
    
    '''hook = BaseHook.get_hook('google_cloud_storage_default')
    client = hook.get_conn()

    bucket = client.bucket(bucket_name)'''
    # Upload Parquet data to Google Cloud Storage
    #storage_client = storage.Client()
    
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(remote_blob_name_)
    #blob.upload_from_filename(local_file_path)
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
    
def read_parquet_from_gcs(bucket_name, remote_blob_name, credentials):
    """Reads a Parquet file from Google Cloud Storage."""
    # Download Parquet file from GCS
    current_date_str = datetime.now().strftime("%Y%m%d")
    remote_blob_name_ = f"{current_date_str}/{remote_blob_name}"
    
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(remote_blob_name_)
    parquet_data = blob.download_as_bytes()

    # Read Parquet file using pyarrow
    parquet_buffer = BytesIO(parquet_data)
    parquet_table = pq.read_table(parquet_buffer)

    return parquet_table    

   
def load_data(gcs_bucket, blob_name, creds):
    db_url = os.environ['DB_URL']
    engine = sqlalchemy.create_engine(db_url)
    print('In function load_data()\nWriting output file')
    #df.to_sql("outcomes", conn, if_exists="append", index=False)
    #df.to_sql("outcomes", conn, if_exists="replace", index=False)
    
    # def insert_on_conflict_nothing(table, conn, keys, data_iter):
    #     # "key" is the primary key in "conflict_table"
    #     data = [dict(zip (keys, row)) for row in data_iter]
    #     stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=[key])
    #     result = conn.execute(stmt)
    #     return result.rowcount

    # pd.read_parquet(table_file).to_sql(table_name, engine, if_exists="replace", index=False)
    # print(table_name+" loaded")
    
    parquet_table = read_parquet_from_gcs(gcs_bucket, blob_name, creds)
    outcomes = parquet_table.to_pandas()
    outcomes_cols = []
    for c in outcomes.columns:
        outcomes_cols.append(c.replace(" ", "_").lower())
    table_name = blob_name.replace(".parquet", "") 
    outcomes.to_sql(table_name, engine, if_exists="replace", index = False)
    print(table_name+" loaded")
    
    # with engine.connect() as conn:
    #     with open("load_data.sql") as file:
    #        # print('Hi')
    #         query = sqlalchemy.sql.text(file.read())
    #         print(query)
    #         conn.execute(query)
    # with engine.connect() as conn:
    #     with open("sql_queries.sql") as file:
    #         #print('Hi')
    #         query = sqlalchemy.sql.text(file.read())
    #         print(query)
    #         conn.execute(query)

def transform_data(source_csv, gcs_bucket, blob_name, creds):   

    #df = pd.read_csv(source_csv)
    parquet_table = read_parquet_from_gcs(source_csv, blob_name, creds)
    df = parquet_table.to_pandas()

    print('In function transform_data()')
    #df['Date'] = df['DateTime'].str.split(' ').str[0]
    df['Reproductive Status upon Outcome'] = df['Sex upon Outcome'].str.split(' ').str[0]
    df['Sex upon Outcome'] = df['Sex upon Outcome'].str.split(' ').str[1]
    df['Sex upon Outcome'].fillna('Unknown',inplace=True)
    df['Reproductive Status upon Outcome'].fillna('Unknown',inplace=True)
    df['Date of Outcome'] = pd.to_datetime(df['DateTime'],format='mixed').dt.date
    df['Date of Birth'] = pd.to_datetime(df['Date of Birth'],format='mixed').dt.date
    df['Years Age upon Outcome'] = ((df['Date of Outcome'] - df['Date of Birth']) / np.timedelta64(365, 'D')).astype('int')
    df.drop(columns=['DateTime','MonthYear', 'Age upon Outcome'],inplace=True)
    df['Outcome Type'].fillna('-',inplace=True)
    df['Outcome Subtype'].fillna('-',inplace=True)
    #print('Now returning to main()')

    #Path(target_dir).mkdir(parents=True, exist_ok=True)

    #df.to_parquet(target_dir+'outcomes.parquet')
    upload_df_to_gcs(df, gcs_bucket, "outcomes.parquet",credentials=creds)
    #return df

def extract_data(source_url, gcs_bucket, creds):
    
    data = pd.read_csv(source_url)
    upload_df_to_gcs(data, gcs_bucket, "outcomes.parquet",credentials=creds)