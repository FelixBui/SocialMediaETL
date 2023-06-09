from class_YouTube import YouTubeETL
from typing import Optional

class CaptionETL:

    def __init__(self, video_url: str, sa_key: str, bucket_name: Optional[str]):
        super.__init__(self, video_url, sa_key, bucket_name)

    def extract(self):
        pass

    def transform(self) -> list:
        video_id=youtube.video_id
        data_list=[]
        for idx, caption_track in enumerate(youtube.caption_tracks):
            caption_id=f"{file_name}_{str(idx+1)}"
            language=caption_track.name
            content=youtube.captions[caption_track.code].json_captions

            # Create a dictionary to store the data
            data = {
                "Caption_id": caption_id,
                "VideoID": video_id,
                "Language": language,
                "Contents": content
            }
            data_list.append(data)
        return data_list
        
    def load(self, data_list: list, folder = "Caption", content_type = "application/json"):
        super.load(data_list,folder,content_type)