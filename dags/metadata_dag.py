from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pytube import YouTube
from pytube import Search
from google.cloud import storage
from data.transformed.youtube.ingestion_metadata_video import *
from data.transformed.youtube.ingestion_metadata_channel import *
from data.transformed.youtube.ingestion_metadata_caption import *
from data.transformed.youtube.ingestion_metadata_thumbnail import *



# Define the DAG arguments
default_args = {
    "owner": "tmq",
    "depends_on_past": False,
    "email": ['shenkedokato@gmail.com'] ,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the function to retrieve video metadata and upload to GCS
def retrieve_video_metadata():
    search_list = Search('meme')
# search_list.get_next_results()
    for search in search_list.results:
        video_id=search.vid_info["videoDetails"]["videoId"]
        video_url=f"https://www.youtube.com/watch?v={video_id}"
        try:
            metadata_video=Ingestion_Metadata_Video(video_url).execute()
            metadata_channel=Ingestion_Metadata_Channel(video_url).execute()
            metadata_caption=Ingestion_Metadata_Caption(video_url).execute()
            metadata_thumbnail=Ingestion_Metadata_Thumbnail(video_url).execute()
        except: 
            continue

    

# Define the DAG
with DAG(dag_id='youtube_metadata_ingest_dag', default_args=default_args, schedule_interval="0 21 * * *",start_date=datetime(2023, 1, 3, 22, 0),catchup=False) as dag:
    # Define the task to retrieve video metadata and upload to GCS
    ingest_video = PythonOperator(
        task_id='ingest_video_metadata',
        python_callable=retrieve_video_metadata
    )

# Set the task dependencies
ingest_video

