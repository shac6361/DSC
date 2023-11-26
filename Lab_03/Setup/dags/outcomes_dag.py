from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
from etl_scripts.pipeline import transform_data
from etl_scripts.pipeline import load_data
from etl_scripts.pipeline import extract_data
from datetime import date
import json
from google.oauth2 import service_account

creds = {
  "type": "service_account",
  "project_id": "orbital-ship-406320",
  "private_key_id": "e99b89a17b9a8e9d39bb3e3dfb26ad4f1c93b6cd",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDjHkA6OBvnY4IL\nw1BD1yKTZrji76ex0w/AhF1fgQFXZwnilm2BS7kpb/z+SSUgDHEh7bn5kQoeMebx\nVlauiuWauSXsTmpCJWxLzP6aH5tXJz06jNp7WVqvQ2YF6l41PEzw1OFiWbsHsCiH\n0MiFbJUlEqoINblAec/oorQhCcWzRYxcx7xTgtQdu41pPTBPuL75fuV8ruXZ+FpD\n/fTkXMkRHLUlFld+sBFnNM4CJdh+CiX+kiRzN5X94UAL0SRYDTcOZUF3Zppp22y+\nyIPC9XZdnXMA/GxBj38TwnCspa3/vAjQdkPnwh0lQGGk90dKiU1CqawQZQgO9DAi\nQAieUQwBAgMBAAECggEAEAGlYsHQxaGWHFO39NZuq2P4vi9IHeVs6mAc+VdTZUa8\nkM/4bvC+35RZVWdWSuHvMmyMrnGlL7RlcFDQOLdO40HbnsHc0niZhbLjprBF95ur\nJwgFeRGdE7xBe0Olpauje72lFmD4jqj0BYGWmdpDNwlC6BbRM+57bQvUFUsynowZ\n1a5bDh32PGu/0I924ygKTSQSfZeOvZYtrOkhQvlo5tL06o22ufPdrEKdcekcf5YX\n3hbdcAtEjE//Pyy/GQD93X0TN6r5pvt4ksqTOTvQNBTOcKcIbVJJNnyxpESPqxlN\ns5rFk+DEYe9Sg5wgeitj/O5Hmgv5DmyEfkuv5bQ6WQKBgQD8DXjpsawVkO+qeNvS\nQkT4diogcWVbqrftcftZNoCPF1wkTRJt1+I8aGBi8CXOZR+HwbBzX25cQI+H+v/n\nb5uh1q5T0RsUtDSq5wVlCIvtFvd7e3AQljdz9feoW1NXrXDzNv+D13KcvD7wxVIw\nLBampuh1EQKKoXmJtstRgPv86wKBgQDmrM+/EvCCP3H5F5Kut8d/jAumgxe6GPxy\nlzyNN+ZBGfkz+kFuRTjYsNz4WrtsB2+JdERDxZjF2066cIEqf5LKREFR/s9oEf6q\nLDB8L7lhYBaQGvBDkqD/kTGI1dJwxGqQ7TgTbqXoJneEWpwmnydOuthDujcE+g/q\ne1HgB8DvwwKBgQDQqJ6GaC2blOaza2YfQ/kw7zGktftAza2CBjAcBMCctKv06VDu\nWh/R/l58yW2i1ZgqnK/kcwY7nL61xZaAE6b1bXHXtW4Dz8MiECopH9AmsDKkqOV2\n9cRmXSNu3lu6Y7zzu33/uSYKltRMIi4N7xG5WMdr0m8WMOManJbonCz/jQKBgQCh\n7xjcAviyBsYE3oaLftpe8LpRB2BFtTqrWv4YCPDsLMajtX/vYEra27YT5uYBnWqw\nbN1rUjDAXQd0nHwgkHRE6AuO+zkwzskfP2w+EeaqtCDCwxZ96VZ6crv6Wlij7078\nNHuUMK3pPe/TCyTpe/rFKyeczAHJzZ4UtsOwaOEJrwKBgQCZcCJUJX741z+6IfZ1\nbuyPBD+b860bCWdIyjWaalPAOlHknj9CsyV+EUDbQxH1VjXEXxC4BKueblplLr0Q\ntjhGFoYEvdGoZQpPdvnQ5DnVVn71mHfzrLkHXd4vYvT+2+GhEBMCCXqYYCAYPHM9\n9GCeHwYls7L3tcVx9ow74HGy9w==\n-----END PRIVATE KEY-----\n",
  "client_email": "dsc-69@orbital-ship-406320.iam.gserviceaccount.com",
  "client_id": "115151461967673009021",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/dsc-69%40orbital-ship-406320.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

cred = service_account.Credentials.from_service_account_info(creds)

SOURCE_URL = "https://data.austintexas.gov/api/views/9t4d-g238/rows.csv?date="+str(date.today())
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
CSV_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/downloads/'
CSV_TARGET_FILE = CSV_TARGET_DIR + 'outcomes_{{ ds }}.csv'
PQ_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/processed/'

with DAG(
    dag_id="outcomes_dag",
    start_date=datetime(2023,11,1),
    schedule_interval='@daily'
) as dag:
    
    # extract = BashOperator(
    #     task_id="extract",
    #     bash_command=f'curl --create-dirs -o {CSV_TARGET_FILE} {SOURCE_URL}',
    # )
    
    extract = PythonOperator(
        task_id="extract",
        python_callable = extract_data,
        op_kwargs = {
            'source_url': SOURCE_URL,
            'gcs_bucket': 'raw_shelter_data_csv',
            'creds': cred
        }
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs = {
            'source_csv':'raw_shelter_data_csv',
            'gcs_bucket': 'processed_shelter_data_parquet',
            'blob_name': 'outcomes.parquet',
            'creds': cred
        }
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
        op_kwargs = {
            'gcs_bucket': 'processed_shelter_data_parquet',
            'blob_name': 'outcomes.parquet',
            'creds': cred,
            #'key': 'outcome_id' 
        }
    )
    extract >> transform >> load