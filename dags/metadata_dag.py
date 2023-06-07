from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pytube import YouTube
from google.cloud import storage

# Define the DAG arguments
default_args = {
    'owner': 'shenkedokato',
    'start_date': datetime(2023, 1, 1),
}

# Define the function to retrieve video metadata and upload to GCS
def retrieve_video_metadata(url, bucket_name, file_name):
    # Download the YouTube video and get its metadata
    video = YouTube(url)
    metadata = video.vid_info

    # Initialize the GCS client
    client = storage.Client()

    # Get the GCS bucket and create a new blob
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Convert the metadata to bytes and upload it to GCS
    metadata_bytes = str(metadata).encode()
    blob.upload_from_string(metadata_bytes)

# Define the DAG
with DAG('youtube_ingest_dag', default_args=default_args, schedule_interval=None) as dag:
    # Define the task to retrieve video metadata and upload to GCS
    ingest_video = PythonOperator(
        task_id='ingest_video_metadata',
        python_callable=retrieve_video_metadata,
        op_args=['https://www.youtube.com/watch?v=1XoICkGxWtw', 'video_storage_yt', 'metadata']
    )

# Set the task dependencies
ingest_video

