from data.raw.youtube.ingestion_metadata_video import *
from data.raw.youtube.ingestion_metadata_channel import *
from data.raw.youtube.ingestion_metadata_caption import *
from data.raw.youtube.ingestion_metadata_thumbnail import *


if __name__ == '__main__':
    video_url="https://www.youtube.com/watch?v=ZtBzWUZbTvA"
    metadata_video=Ingestion_Metadata_Video(video_url).execute()
    metadata_channel=Ingestion_Metadata_Channel(video_url).execute()
    metadata_caption=Ingestion_Metadata_Caption(video_url).execute()
    metadata_thumbnail=Ingestion_Metadata_Thumbnail(video_url).execute()

