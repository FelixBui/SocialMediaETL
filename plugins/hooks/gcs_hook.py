from airflow.providers.google.cloud.hooks.gcs import GoogleCloudStorageHook

class MyGCSHook(GoogleCloudStorageHook):
    def __init__(self, *arg, **kwargs) -> None:
        super().__init__(*arg, **kwargs)
        
    def upload(self, bucket_name, file_name, destination_blob_name):
        return GoogleCloudStorageHook.upload(bucket_name, file_name, destination_blob_name)
