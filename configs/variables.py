### GCP variables
GCP_SA_KEY = ''  #Enter gcp service account path

### GCS variables
YTB_BUCKET_NAME = 'video_storage_yt'  #Enter you bucket name
folder = {
    'fd_video' : 'Video',
    'fd_caption' : 'Caption',
    'fd_thumbnail' : 'Thumbnail',
    'fd_channel' : 'Channel',
}
### Content type for load to gcs
CONTENT_TYPE = {
    'json': 'application/json',
    'mp4': 'video/mp4'
}
### Airflow config:
AIRFLOW_HOME = "/opt/airflow"