import os
from pytube import YouTube
import pytube
from pathlib import Path
from moviepy.editor import *
from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

default_args = {
    "owner": "shen",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
    "email": ['shenkedokato@gmail.com'] ,
    "sla": timedelta(hours=1),
    'email_on_failure': False,
    'email_on_retry': False,
}
dag = DAG(
    dag_id="download_video_from_yt",
    default_args=default_args,
    description= 'dag download video from youtube',
    schedule_interval = "@once",
    start_date=datetime(2023, 5, 15),
    catchup=False,
    tags=["testing"]
) 

def download_video():
    video_id = '5gfS8MydNLk'
    video_url = f'https://www.youtube.com/watch?v={video_id}'
    # Download YouTube video in 480p resolution using pytube
    yt = YouTube(video_url).streams.get_by_itag(22).download()
    name_file_mp4 = f'{video_id}.mp4'
    os.rename(yt, name_file_mp4)


with dag:
    start_task = DummyOperator(task_id = "start")
    end_task = DummyOperator(task_id = "end")

    first_task = PythonOperator(task_id = "hello_word", python_callable=download_video)
    start_task >> first_task >> end_task



