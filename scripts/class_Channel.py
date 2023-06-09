from class_YouTube import YouTubeETL
from typing import Optional

class ChannelETL(YouTubeETL):

    def __init__(self, video_url: str , sa_key: str, bucket_name: Optional [str] ):
        super.__init__(self,video_url,sa_key, bucket_name)

    def extract(self):
        pass

    def transform(self):
        channel_id=channel.channel_id
        channel_url=channel.channel_url
        channel_name=channel.channel_name
        channel_description=channel.initial_data["metadata"]['channelMetadataRenderer']['description']
        channel_keywords=channel.initial_data["metadata"]['channelMetadataRenderer']['keywords']
        channel_subcribed=channel.initial_data["header"]["c4TabbedHeaderRenderer"]["subscriberCountText"]['simpleText']
        # Create a dictionary to store the data
        data = {
            "ChannelID": channel_id,
            "URL": channel_url,
            "Name": channel_name,
            "Description": channel_description,
            "Subcribed": channel_subcribed,
            "Keywords": channel_keywords
        }
        data_list=[]
        data_list.append(data)
        return data_list

    def load(self, data_list: list, folder: Optional = "Channel", content_type: Optional = "application/json"):
        super.load(data_list,folder,content_type)



"""
    Code ẩu, không import thư viện vào thì dùng class con dùng kiểu gì ??? thừa kế của thằng cha nhưng không import thằng cha vào để thừa kế ??
"""