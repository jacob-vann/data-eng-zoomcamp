import config.config as cfg

from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

GCS_BUCKET = cfg.GCS_BUCKET
PROJECT_ID = cfg.PROJECT_ID
BQ_DATASET = cfg.BQ_DATASET
BQ_TABLE = cfg.BQ_TABLE
BQ_PARTITIONED_TABLE = cfg.BQ_PARTITIONED_TABLE
SCHEMA_BQ_FILE = cfg.SCHEMA_BQ_FILE

CREATE_PARTITIONED_TABLE_QUERY = f'CREATE OR REPLACE TABLE {BQ_DATASET}.{BQ_PARTITIONED_TABLE} \
                                 PARTITION BY RANGE_BUCKET(OBJECTID, GENERATE_ARRAY(0, 100, 10)) AS \
                                 SELECT * FROM {BQ_DATASET}.{BQ_TABLE};'

      
dag = DAG(
    'gcs_to_bq_dag',
     schedule_interval='@yearly',
     start_date=datetime(2018, 1, 1),
     end_date=datetime(2020, 1, 1),
     catchup=True,
     max_active_runs=1
)
with dag:

    gcs_2_bq_task =  BigQueryCreateExternalTableOperator(
        task_id='gcs_2_bq_task',
        table_resource={
            'tableReference':{
                'projectId': PROJECT_ID,
                'datasetId': BQ_DATASET,
                'tableId': BQ_TABLE
            },
            'externalDataConfiguration':{
                'autodetect': "True",
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{GCS_BUCKET}/raw/*']
            },
        },
    )

    bq_create_partition_table_task  = BigQueryInsertJobOperator(
        task_id=f'bq_create_partitioned_table_task',
        configuration={
            'query': {
                'query': CREATE_PARTITIONED_TABLE_QUERY,
                'useLegacySql': False
            }
        }
    )

    gcs_2_bq_task >> bq_create_partition_table_task
    
    
    
    
    
    
    
    
    
    
    
    
    
    


















