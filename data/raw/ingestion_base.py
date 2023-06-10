from datetime import datetime, date
import logging


from data.socialmediaetl_base import SocialMediaETL
from plugins.helpers.utils import *

PREFIX = [date.today(), datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
class Ingestion(SocialMediaETL):
    def __init__(self, source: str):
        super().__init__()
        self.bucket = get_gcs_bucket()
        self.source = source

    def extract(self):
        pass
    
    def transform(self):
        pass

    def load(self, source_file_path: str, destination_blob_name: str, content_type: str):
        # Create a GCS blob
        blob = self.bucket.blob(destination_blob_name)

        # Set the content type of the blob
        blob.content_type = content_type

        # Upload the mp4 data to GCS
        blob.upload_from_filename(source_file_path)

        # logging.info(f"Data ingested and saved to GCS: gs://{YTB_BUCKET_NAME}/{file_path}")
        print(f"Ingested {destination_blob_name}")

    def execute(self):
        return super().execute()

