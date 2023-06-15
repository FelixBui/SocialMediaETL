GCP_SA_KEY = 'C:/Users/Admin/Downloads/sa-key-SMETL.json'
folder = {
    'fd_video' : 'Video',
    'fd_caption' : 'Caption',
    'fd_thumbnail' : 'Thumbnail',
    'fd_channel' : 'Channel',
}
YTB_BUCKET_NAME = 'video_storage_yt'
content_type = {
    'json': 'application/json',
    'mp4': 'video/mp4'
}
PROJECT_ID = "socialmediaetl-386712"
DATASET_ID = 'socialmediaetl_dwh'
TABLE_ID = {
    "Video":"Metadata_video",
    "Channel":"Metadata_channel",
    "Caption":"Metadata_caption",
    "Thumbnail":"Metadata_thumbnail",
}

