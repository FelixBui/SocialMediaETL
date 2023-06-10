from data.transformed.ingestion_metadata import Ingestion_Metadata
from configs.variables_1 import *

class Ingestion_Metadata_Video(Ingestion_Metadata):
    def __init__(self,video_id):
        super().__init__(video_id)
    def extract(self):
        pass 
    def transform(self) -> list:
        youtube = self.youtube
        channel = self.channel
        video_id=youtube.video_id
        channel_id = youtube.channel_id
        length=youtube.length
        publish_date=str(youtube.publish_date.year) + "/" + str(youtube.publish_date.month) + "/" + str(youtube.publish_date.day)
        title=youtube.title
        view=youtube.views
        description=None
        for vd_id in channel.initial_data["contents"]['twoColumnBrowseResultsRenderer']['tabs'][1]['tabRenderer']['content']['richGridRenderer']['contents']:
        #     print(vd_id)
            video_id_ch = vd_id.get('richItemRenderer', {}).get('content', {}).get('videoRenderer', {}).get('videoId')
            if video_id == video_id_ch:
                description = vd_id.get('richItemRenderer', {}).get('content', {}).get('videoRenderer', {}).get('descriptionSnippet', {}).get('runs', [])[0].get('text')
        # Create a dictionary to store the data
        data = {
            "VideoID": video_id,
            "ChannelID": channel_id,
            "Length": length,
            "Publish_date": publish_date,
            "Title": title,
            "Views":view,
            "Description":description
        }
        data_list=[]
        data_list.append(data)
        return data_list
    def load(self, data_list: list, folder = folder["fd_video"], content_type = content_type["json"]):
        super().load(data_list,folder,content_type)
    
    def execute(self):
        transform_data = self.transform()
        self.load(transform_data)
        return super().execute()



