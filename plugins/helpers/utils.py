from google.oauth2.service_account import Credentials
from google.cloud import storage
from configs.variables import GCP_SA_KEY,BUCKET_NAME


def get_gcp_creds():
    '''
        Take path to Service Account key and return credentials object
    '''
    return Credentials.from_service_account_file(GCP_SA_KEY)

def get_gcs_bucket():
    credentials = get_gcp_creds()
    storage_client = storage.Client(credentials=credentials)
    return storage_client.get_bucket(BUCKET_NAME)
