from typing import Optional
import json
import logging


from data.socialmediaetl_base import SocialMediaETL
from configs.variables import *
from plugins.helpers.utils import get_gcs_bucket


class Ingestion(SocialMediaETL):
    def __init__(self, video_url):
        super().__init__()
        self.video_url = video_url
        self.bucket = get_gcs_bucket()
        self.file_name = self.youtube.video_id

    def extract(self):
        pass
    
    def transform(self):
        pass

    def load(self, data_list: list, folder: str, content_type: str):
        for idx, data in enumerate(data_list):
            json_data = json.dumps(data)
            # Get the GCS bucket
            file_path = f"{folder}/{self.file_name}_{str(idx+1)}.json"
            # Create a GCS blob
            blob = self.bucket.blob(file_path)

            # Set the content type of the blob
            blob.content_type = content_type

            # Upload the JSON data to GCS
            blob.upload_from_string(json_data, content_type=content_type)

            logging.info(f"Data ingested and saved to GCS: gs://{YTB_BUCKET_NAME}/{file_path}")

    def execute(self):
        return super().execute()

