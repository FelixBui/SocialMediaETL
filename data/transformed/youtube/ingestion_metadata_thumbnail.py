from data.transformed.ingestion_metadata import Ingestion_Metadata
from configs.variables_1 import *
import time

class Ingestion_Metadata_Thumbnail(Ingestion_Metadata):
    def __init__(sefl,video_id):
        super().__init__(video_id)
    def extract(self):
        pass 
    def transform(self) -> list:
        file_name = self.file_name
        youtube = self.youtube
        video_id=youtube.video_id
        channel_id=None
        thumbnail_url=None
        thumbnail_width=None
        thumbnail_height=None
        data_list=[]
        count =0
        for thumbnail in youtube.vid_info["videoDetails"][ 'thumbnail']['thumbnails']:
            count+=1
            thumbnail_id=f"{file_name}_{str(count)}"
            thumbnail_url=thumbnail["url"]
            thumbnail_width=thumbnail["width"]
            thumbnail_height=thumbnail["height"]
            current_timestamp = time.time()
            data = {
                "ThumbnailID": thumbnail_id,
                "VideoID": video_id,
                "URL": thumbnail_url,
                "Width": thumbnail_width,
                "Height": thumbnail_height,
                "Timestamp": current_timestamp 
            }

            data_list.append(data)
        return data_list

    def load(self, data_list: list, folder = folder["fd_thumbnail"], content_type = content_type["json"]):
        super().load(data_list,folder,content_type)
    
    def execute(self):
        transform_data = self.transform()
        self.load(transform_data)
        return super().execute()

