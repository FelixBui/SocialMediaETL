import os
from google.cloud import storage
from pathlib import Path
from google.oauth2 import service_account

# Set up credentials
credentials = service_account.Credentials.from_service_account_file('C:/Users/Admin/Downloads/sa_key.json')
storage_client = storage.Client(credentials=credentials)
bucket_name = 'video_storage_yt'
bucket = storage_client.get_bucket(bucket_name)

# Set up YouTube video ID and URL
video_id = '5gfS8MydNLk'
video_file = f'{video_id}.mp4'
print(video_file)

blob = bucket.blob(video_file)
with open("video_file",'rb') as f:
    blob.upload_from_filename(f)
    # Delete local video file
    os.remove(video_file)
    print(f"Đã xóa file {video_file} thành công.")




