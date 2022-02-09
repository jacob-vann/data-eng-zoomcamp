import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

CREATE_PARTITION_TABLE_QUERY = f"CREATE OR REPLACE TABLE trips_data_all.yellow_tripdata_partitoned \
                                  PARTITION BY DATE(tpep_pickup_datetime) AS \
                                  SELECT * FROM {BIGQUERY_DATASET}.external_yellow_tripdata;"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    dag_id="gcs_to_bq_dag",
    schedule_interval="@daily",
    default_args = default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de']

)


for colour in ['yellow', 'green']:
    with dag:

            gcs_2_gcs_task = GCSToGCSOperator(
                    task_id=f"move_{colour}_tripdata_files_task",
                    source_bucket=BUCKET,
                    source_object=f'raw/{colour}_tripdata_*.parquet',
                    destination_bucket=BUCKET,
                    destination_object=f"{colour}/"
                )

            gcs_2_bq_ext_task =  BigQueryCreateExternalTableOperator(
                        task_id=f"bq_{colour}_tripdata_external_table_task",
                        table_resource={
                            "tableReference": {
                                "projectId": PROJECT_ID,
                                "datasetId": BIGQUERY_DATASET,
                                "tableId": f"external_{colour}_tripdata",
                            },
                            "externalDataConfiguration": {
                                "sourceFormat": "PARQUET",
                                "sourceUris": [f"gs://{BUCKET}/{colour}/*"],
                            },
                        },
                    )

            bq_create_partition_table = BigQueryInsertJobOperator(
                        task_id=f"bq_create_{colour}_tripdat_partitioned_table_task",
                        configuration={
                            "query": {
                                "query": CREATE_PARTITION_TABLE_QUERY,
                                "useLegacySql": False,
                            }
                        })

            gcs_2_gcs_task >> gcs_2_bq_ext_task >> bq_create_partition_table