from pytube import YouTube
from pytube import Channel
from pytube import Caption
import json
from google.cloud import storage
from google.oauth2 import service_account
from typing import Optional

class SocialMediaETL:
    
    def __init__(self, video_url: str, sa_key: str, bucket_name: Optional[str] = "video_storage_yt"):
        sefl.video_url=video_url
        sefl.bucket_name=bucket_name
        sefl.file_name=file_name
        credentials = service_account.Credentials.from_service_account_file(sa_key)
        storage_client = storage.Client(credentials=credentials)
        self.bucket = storage_client.get_bucket(bucket_name)

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass