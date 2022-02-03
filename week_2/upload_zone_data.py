import os

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingestion_functions import format_to_parquet, upload_file_to_gcs

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
OUTPUT_FILE = AIRFLOW_HOME + '/output_file' + '.csv'
PARQUET_FILE = OUTPUT_FILE.replace('.csv', '.parquet')
GCS_FILE =  'taxi_zone.parquet'

BUCKET = os.environ.get("GCP_GCS_BUCKET")

dag = DAG(
    "upload_zone_data",
     schedule_interval="0 6 1 * *",
     start_date=datetime(2019, 1, 1),
     catchup=True,
     max_active_runs=3
)

with dag:

    download_data = BashOperator(
        task_id='download_data',
        bash_command=f'curl -sSL {URL} > {OUTPUT_FILE}'
    ),

    format_file_to_parquet = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f'{OUTPUT_FILE}',
        })

    local_to_gcs = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_file_to_gcs,
        op_kwargs={"bucket": BUCKET, "object_name": GCS_FILE,
                   "local_file": PARQUET_FILE}
    )

    remove_temp_files = BashOperator(
        task_id='remove_temp_files',
        bash_command=f'rm {OUTPUT_FILE} {PARQUET_FILE}'
    )

download_data >> format_file_to_parquet >> local_to_gcs >> remove_temp_files