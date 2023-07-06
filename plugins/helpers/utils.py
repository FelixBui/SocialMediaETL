import os
import glob
from google.oauth2.service_account import Credentials
from google.cloud import storage,bigquery
from configs.variables import GCP_SA_KEY,YTB_BUCKET_NAME,PROJECT_ID,DATASET_ID
from datetime import datetime

def get_gcp_creds() -> object:
    '''
        Take path to Service Account key and return credentials object
    '''
    return Credentials.from_service_account_file(GCP_SA_KEY)

def get_gcs_bucket() -> object:
    credentials = get_gcp_creds()
    storage_client = storage.Client(credentials=credentials)
    return storage_client.get_bucket(YTB_BUCKET_NAME)

def create_folder(bucket: object, destination_folder_name: str):
    folder_exists = any(blob.name == f'{destination_folder_name}' for blob in bucket.list_blobs())
    if folder_exists is False:
        blob = bucket.blob(destination_folder_name)
        blob.upload_from_string('')
        print('Created {} .'.format(destination_folder_name))
    else:
        pass

def get_base_name(path: str):
    '''
        Return file name of a file directory
    '''
    return os.path.basename(path)

def get_dir_name(path: str):
    '''
        Return directory name of file directory
    '''
    return os.path.dirname(path)

def clear_mp4_files():
    '''
        Intensively clean .mp4 files in Airlow home
    '''
    airflow_home = os.environ['AIRFLOW_HOME']
    mp4_list = glob.glob(airflow_home + "/*.mp4")
    for mp4 in mp4_list:
        os.remove(mp4)
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