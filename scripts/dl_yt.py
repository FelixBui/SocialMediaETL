from pytube import YouTube
import pytube

import os

video_id= '2tDgviJoYEo'
video_url = f'https://www.youtube.com/watch?v={video_id}'
file_name= f'{video_id}.mp4'
yt = YouTube(video_url).streams.get_by_itag(22).download(filename=file_name)
