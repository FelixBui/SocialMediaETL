from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from data.transformed.youtube.load_metadata_video_bigquery import *
from data.transformed.youtube.load_metadata_channel_bigquery import *
from data.transformed.youtube.load_metadata_thumbnail_bigquery import *
from data.transformed.youtube.load_metadata_caption_bigquery import *



# Define the DAG arguments
default_args = {
    'owner': 'shenkedokato',
    'start_date': datetime(2023, 1, 1),
}

# Define the function to retrieve video metadata and upload to GCS
def load_metadata_bigquery():
    load_metadata_video_bigquery = Load_Metadata_Video_Bigquery().execute()
    load_metadata_channel_bigquery = Load_Metadata_Channel_Bigquery().execute()
    load_metadata_thumbnail_bigquery = Load_Metadata_Thumbnail_Bigquery().execute()
    load_metadata_caption_bigquery = Load_Metadata_Caption_Bigquery().execute()

# Define the DAG
with DAG('youtube_metadata_dag_bigquery', default_args=default_args, schedule_interval="@daily") as dag:
    # Define the task to retrieve video metadata and upload to GCS
    ingest_video = PythonOperator(
        task_id='load_video_metadata_bigquery',
        python_callable=load_metadata_bigquery
    )

# Set the task dependencies
ingest_video

