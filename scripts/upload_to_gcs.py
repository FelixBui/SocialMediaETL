import io
from io import BytesIO
from google.cloud import storage
storage_client =storage.Client.from_service_account_json("C:/Users/Admin/Downloads/sa-key-SMETL.json")
bucket=storage_client.get_bucket("video_storage_yt")
filename="5gfS8MydNLk.mp4"
blob=bucket.blob(filename)
with open('5gfS8MydNLk.mp4','rb') as f:
    blob.upload_from_file(f)
print("completed")