from datetime import datetime
from plugins.helpers.utils import *
from google.cloud import bigquery
from configs.variables_1 import *

def datetime_now():
    now = datetime.now()
    day = now.day
    month = now.month
    year = now.year
    return f"{year}-{month}-{day}"

def get_bigquery_client() -> object:
    credentials = get_gcp_creds()
    bq_client = bigquery.Client(credentials=credentials,project=PROJECT_ID)
    return bq_client
def get_bigquery_dataset() -> object:
    credentials = get_gcp_creds()
    bq_client = bigquery.Client(credentials=credentials,project=PROJECT_ID)
    dataset_ref = bq_client.dataset(DATASET_ID)    
    return dataset_ref
    

