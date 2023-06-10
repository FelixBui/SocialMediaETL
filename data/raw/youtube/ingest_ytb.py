import os
from datetime import datetime, date
import requests
import logging

from configs.variables import CONTENT_TYPE, AIRFLOW_HOME
from data.raw.ingestion_base import Ingestion, PREFIX
from plugins.helpers.utils import get_gcs_bucket, get_dir_name, get_base_name

import pytube
from pytube import YouTube


class Ingest_YTB(Ingestion):
    def __init__(self, source: str):
        super().__init__(source)
        self.bucket = get_gcs_bucket()
        self._output_path = AIRFLOW_HOME
        
    def _is_age_restricted(self, video_url: str) -> bool:
        '''
            Return True if video is age restricted
        '''
        return YouTube(video_url).age_restricted
    
    def _is_downloaded(self, video_url: str) -> bool:
        '''
            Return True if video is downloaded
        '''
        video_id = YouTube(video_url).video_id
        return any(True for blob in self.bucket.list_blobs() if video_id in blob.name)
    
    def _not_itag_22(self, video_url: str) -> bool:
        '''
            Return True if there no stream with itag 22
        '''
        try:
            streams = [stream for stream in YouTube(video_url).streams.filter(progressive=True, file_extension='mp4')]
            return not any(stream.itag == 22 for stream in streams)
        except Exception as e:
            print(f'{TypeError(e)}')
            return True

    def extract(self, video_url: str, src_file_path: str):
        output_path = get_dir_name(src_file_path)
        file_name = get_base_name(src_file_path)
        return YouTube(video_url).streams.get_by_itag(22).download(output_path, file_name)
    
    def transform(self):
        pass

    def load(self, src_file_path: str, content_type=CONTENT_TYPE['mp4']):
        file_name = f"{PREFIX[1]}_{get_base_name(src_file_path)}"
        dest_blob_name = f"Video/{self.source}/{str(PREFIX[0])}/{file_name}"
        print(f"destination_blob_name: {dest_blob_name}")
        return super().load(src_file_path, dest_blob_name, content_type)        

    def execute(self, video_url: str, src_file_path: str):
        # Define conditions to ingest
        conditions = [self._is_age_restricted(video_url), 
                      self._is_downloaded(video_url),
                      self._not_itag_22(video_url)]
        # Define variables
        # Define your var here, eg. prefix = 1
        # If all conditons are false, launch pipeline
        if all(not condition for condition in conditions):
            # Extract to fs
            self.extract(video_url, src_file_path)
            print("finish extraction")
            # Put to gcs
            self.load(src_file_path)
            print("finish putting to gcs")
        else:
            print("This video is not met all conditions")
            pass
