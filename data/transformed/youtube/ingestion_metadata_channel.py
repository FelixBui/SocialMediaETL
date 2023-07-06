from data.transformed.ingestion_metadata import Ingestion_Metadata
from configs.variables import *
import time
class Ingestion_Metadata_Channel(Ingestion_Metadata):
    def __init__(self,video_id):
        super().__init__(video_id)
    def extract(self):
        pass 
    def transform(self):
        channel = self.channel
        channel_id=channel.channel_id
        channel_url=channel.channel_url
        channel_name=channel.channel_name
        channel_description=channel.initial_data["metadata"]['channelMetadataRenderer']['description']
        channel_keywords=channel.initial_data["metadata"]['channelMetadataRenderer']['keywords']
        channel_subcribed=channel.initial_data["header"]["c4TabbedHeaderRenderer"]["subscriberCountText"]['simpleText']
        current_timestamp = time.time()
        # Create a dictionary to store the data
        data = {
            "ChannelID": channel_id,
            "URL": channel_url,
            "Name": channel_name,
            "Description": channel_description,
            "Subcribed": channel_subcribed,
            "Keywords": channel_keywords,
            "Timestamp": current_timestamp 
        }
        data_list=[]
        data_list.append(data)
        return data_list

    def load(self, data_list: list, folder = folder["fd_channel"], content_type = CONTENT_TYPE["json"]):
        super().load(data_list,folder,content_type)

    def execute(self):
        transform_data = self.transform()
        self.load(transform_data)
        return super().execute()

