import os
import logging
import pandas as pd
import config.config as cfg
import numpy as np

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')
EXECUTION_YEAR = '{{ execution_date.strftime(\'%Y\') }}'
DATA_SET = f'road-traffic-accidents-berlin-{EXECUTION_YEAR}'
ZIP_FILE = DATA_SET + '.zip'
CSV_FILE = f'road_traffic_accidents_berlin_{EXECUTION_YEAR}' + '.csv'
PARQUET_FILE = CSV_FILE.replace('csv', 'parquet')
GCS_BUCKET = cfg.GCS_BUCKET
SCHEMA = cfg.SCHEMA

def format_data(src_file, dst_file):
    if not src_file.endswith('.csv'):
        logging.error('Can only accept source files in csv format')
        return

    df = pd.read_csv(src_file, sep=';', decimal=",", encoding="ISO-8859-1") \
        .rename(columns={'IstSonstig': 'IstSonstige', 'STRZUSTAND': 'USTRZUSTAND'})

    for col, type in SCHEMA.items():
        if col not in df.columns:
            print('here')
            print(col)
            df[col] = np.nan 

    print(df.columns)
    df.astype(dtype=SCHEMA).to_parquet(dst_file)
    
    # to_csv(dst_file, index=False, encoding='iso-8859-1')

dag = DAG(
    'upload_rta_data_dag',
     schedule_interval='@yearly',
     start_date=datetime(2018, 1, 1),
     end_date=datetime(2020, 1, 1),
     catchup=True,
     max_active_runs=1
)
with dag:

    download_data_task = BashOperator(
        task_id='download_data',
        bash_command=f'cd {AIRFLOW_HOME} && kaggle datasets download -d jacobnealvann/{DATA_SET} && unzip {ZIP_FILE}'
    )

    format_data_task = PythonOperator(
        task_id='format_data_task',
        python_callable=format_data,
        op_kwargs={
            'src_file': f'{AIRFLOW_HOME}/{CSV_FILE}',
            'dst_file': f'{AIRFLOW_HOME}/{PARQUET_FILE}'
        }
    )

    local_to_gcs_task  = LocalFilesystemToGCSOperator(
        task_id='local_to_gcs_task',
        src=f'{AIRFLOW_HOME}/{PARQUET_FILE}',
        dst=f'raw/{CSV_FILE}',
        bucket=GCS_BUCKET 
    )

    remove_temp_files_task = BashOperator(
        task_id='remove_temp_files_task',
        bash_command=f'cd {AIRFLOW_HOME} && rm {ZIP_FILE} {CSV_FILE} {PARQUET_FILE}'
    )
  
    download_data_task >> format_data_task >> local_to_gcs_task >> remove_temp_files_task 

