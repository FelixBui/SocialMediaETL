import os
from pytube import YouTube
import pytube
from pathlib import Path
from moviepy.editor import *
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def download_video():
    video_id = '5gfS8MydNLk'
    video_url = f'https://www.youtube.com/watch?v={video_id}'
    # Download YouTube video in 480p resolution using pytube
    yt = YouTube(video_url).streams.get_by_itag(22).download()
    name_file_mp4 = f'{video_id}.mp4'
    os.rename(yt, name_file_mp4)


with DAG(
    dag_id="download_video_from_yt",
    start_date=datetime(2023, 5, 15),
    schedule = "@once",
    catchup=False,
) as dag:
    task = PythonOperator(task_id = "dl_video", python_callable=download_video)



    




