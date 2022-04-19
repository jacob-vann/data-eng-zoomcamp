import json 

from os import path

with open(path.join(path.dirname(path.realpath(__file__)), 'config.json')) as json_file: 
    cfg= json.load(json_file)

GCS_BUCKET = cfg['gcp']['gcs_bucket']
PROJECT_ID = cfg['gcp']['project_id'] 
BQ_DATASET = cfg['gcp']['bq_dataset'] 
BQ_TABLE = cfg['gcp']['bq_table'] 
BQ_PARTITIONED_TABLE = cfg['gcp']['bq_partitioned_table'] 

SCHEMA_FILE = path.join(path.dirname(path.realpath(__file__)), 'schema.json')
SCHEMA_BQ_FILE = path.join(path.dirname(path.realpath(__file__)), 'schema.json')

with open(SCHEMA_FILE) as json_file: 
    SCHEMA = json.load(json_file)











