# from data.transformed.youtube.ingestion_metadata_video import *
# from data.transformed.youtube.ingestion_metadata_channel import *
# from data.transformed.youtube.ingestion_metadata_caption import *
# from data.transformed.youtube.ingestion_metadata_thumbnail import *
# # video_url="https://www.youtube.com/watch?v=S4rNWqzwRTM"

# from pytube import Search
# search_list = Search('meme')
# # search_list.get_next_results()
# for search in search_list.results:
#     video_id=search.vid_info["videoDetails"]["videoId"]
#     video_url=f"https://www.youtube.com/watch?v={video_id}"
#     try:
#         metadata_video=Ingestion_Metadata_Video(video_url).execute()
#         metadata_channel=Ingestion_Metadata_Channel(video_url).execute()
#         metadata_caption=Ingestion_Metadata_Caption(video_url).execute()
#         metadata_thumbnail=Ingestion_Metadata_Thumbnail(video_url).execute()
#     except: 
#         continue

# from data.transformed.youtube.load_metadata_video_bigquery import *
# from data.transformed.youtube.load_metadata_thumbnail_bigquery import *


# load_metadata_video_bigquery = Load_Metadata_Thumbnail_Bigquery().execute()


