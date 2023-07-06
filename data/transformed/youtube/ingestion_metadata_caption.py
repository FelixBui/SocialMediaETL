from data.transformed.ingestion_metadata import Ingestion_Metadata
from configs.variables import *
import time

class Ingestion_Metadata_Caption(Ingestion_Metadata):
    def __init__(sefl,video_id):
        super().__init__(video_id)
    def extract(self):
        pass 
    def transform(self) -> list:
        file_name = self.file_name
        youtube = self.youtube
        video_id=youtube.video_id
        data_list=[]
        for idx, caption_track in enumerate(youtube.caption_tracks):
            caption_id=f"{file_name}_{str(idx+1)}"
            language=caption_track.name
            content=youtube.captions[caption_track.code].json_captions
            current_timestamp = time.time()

            # Create a dictionary to store the data
            data = {
                "Contents_caption_id": caption_id,
                "VideoID": video_id,
                "Language": language,
                "Contents": content,
                "Timestamp": current_timestamp
            }
            data_list.append(data)
        return data_list
        
    def load(self, data_list: list, folder = folder["fd_caption"], content_type = CONTENT_TYPE["json"]):
        super().load(data_list,folder,content_type)
    
    def execute(self):
        transform_data = self.transform()
        self.load(transform_data)
        return super().execute()


