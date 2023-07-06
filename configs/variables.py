### GCP variables
GCP_SA_KEY = '/opt/airflow/sa_key.json'  #Enter gcp service account path

### GCS variables
YTB_BUCKET_NAME = 'video_storage_yt'  #Enter you bucket name

### Content type for load to gcs
CONTENT_TYPE = {
    'json': 'application/json',
    'mp4': 'video/mp4'
}
### Airflow config:
AIRFLOW_HOME = "/opt/airflow"
### Folder in GCS
folder = {
    'fd_video' : 'Video',
    'fd_caption' : 'Caption',
    'fd_thumbnail' : 'Thumbnail',
    'fd_channel' : 'Channel',
}
### Bucket GCS name
YTB_BUCKET_NAME = 'video_storage_yt'
### ProjectID in GCP
PROJECT_ID = "socialmediaetl-386712"
### Dataset Bigquery
DATASET_ID = 'socialmediaetl_dwh'
### TableID Bigquery
TABLE_ID = {
    "Video":"Metadata_video",
    "Channel":"Metadata_channel",
    "Caption":"Metadata_caption",
    "Thumbnail":"Metadata_thumbnail",
}