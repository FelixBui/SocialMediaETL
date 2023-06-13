from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pytube import YouTube
from google.cloud import storage
from data.transformed.youtube.ingestion_metadata_video import *
from data.transformed.youtube.ingestion_metadata_channel import *
from data.transformed.youtube.ingestion_metadata_caption import *
from data.transformed.youtube.ingestion_metadata_thumbnail import *



# Define the DAG arguments
default_args = {
    'owner': 'shenkedokato',
    'start_date': datetime(2023, 1, 1),
}

# Define the function to retrieve video metadata and upload to GCS
def retrieve_video_metadata():
    video_url="https://www.youtube.com/watch?v=S4rNWqzwRTM"
    metadata_video=Ingestion_Metadata_Video(video_url).execute()
    metadata_channel=Ingestion_Metadata_Channel(video_url).execute()
    metadata_caption=Ingestion_Metadata_Caption(video_url).execute()
    metadata_thumbnail=Ingestion_Metadata_Thumbnail(video_url).execute()
    

# Define the DAG
with DAG('youtube_ingest_dag', default_args=default_args, schedule_interval=None) as dag:
    # Define the task to retrieve video metadata and upload to GCS
    ingest_video = PythonOperator(
        task_id='ingest_video_metadata',
        python_callable=retrieve_video_metadata
    )

# Set the task dependencies
ingest_video

