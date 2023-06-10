import os
from google.oauth2.service_account import Credentials
from google.cloud import storage
from configs.variables_1 import GCP_SA_KEY,YTB_BUCKET_NAME


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