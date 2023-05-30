from pytube import YouTube
import pytube

import os

video_id= '1XoICkGxWtw'
video_url = f'https://www.youtube.com/watch?v={video_id}'
file_name= f'{video_id}.mp4'
video_path=f'/opt/airflow/data/yt'
yt = YouTube(video_url).streams.get_by_itag(22).download(output_path=video_path,filename=file_name)
