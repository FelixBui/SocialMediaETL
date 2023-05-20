import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pytube import YouTube
import pytube
from datetime import datetime
from airflow.contrib.operators.ssh_operator import SSHOperator


dag = DAG(
    dag_id="download_video_from_yt",
    start_date=datetime(2023, 5, 20),
    schedule = "@once",
    catchup=False,
)

def download_youtube_video(video_id):
    video_id='5gfS8MydNLk'
    video_url = f'https://www.youtube.com/watch?v={video_id}'
    # Download YouTube video in 480p resolution using pytube
    yt = YouTube(video_url).streams.get_by_itag(22).download(filename=f'{video_id}.mp4')


with dag:
    # tải video từ youtube và lưu trữ trên Compute Engine
    download_task = PythonOperator(
        task_id='download_video',
        python_callable=download_youtube_video,
    )



    




