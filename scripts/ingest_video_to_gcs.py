from pytube import YouTube
from pytube import Channel
from pytube import Caption

import json
from google.cloud import storage
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file('C:/Users/Admin/Downloads/sa-key-SMETL.json')
storage_client = storage.Client(credentials=credentials)
# Define the YouTube video URL and GCS details
def ingest_video_youtube_data(video_url: str, bucket_name: str, file_name: str):
    # Get the YouTube video data
    yt = YouTube(video_url)
    video_id=yt.video_id
    channel_id = yt.channel_id
    length=yt.length
    publish_date=str(yt.publish_date.year) + "/" + str(yt.publish_date.month) + "/" + str(yt.publish_date.day)
    title=yt.title
    view=yt.views
    ch = Channel(yt.channel_url)
    description=None
    i=0
    for vd_id in ch.initial_data["contents"]['twoColumnBrowseResultsRenderer']['tabs'][1]['tabRenderer']['content']['richGridRenderer']['contents']:
    #     print(vd_id)
        video_id_ch = vd_id.get('richItemRenderer', {}).get('content', {}).get('videoRenderer', {}).get('videoId')
        if video_id == video_id_ch:
            description = vd_id.get('richItemRenderer', {}).get('content', {}).get('videoRenderer', {}).get('descriptionSnippet', {}).get('runs', [])[0].get('text')
            print(description)
    # Create a dictionary to store the data
    data = {
        "VideoID": video_id,
        "ChannelID": channel_id,
        "Length": length,
        "Publish_date": publish_date,
        "Title": title,
        "Views":view
    }

    # Convert the data to JSON
    json_data = json.dumps(data)

    # Create a GCS client

    # Get the GCS bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Define the GCS file path
    file_path = f"Video/{file_name}.json"

    # Create a GCS blob
    blob = bucket.blob(file_path)

    # Set the content type of the blob
    blob.content_type = "application/json"

    # Upload the JSON data to GCS
    blob.upload_from_string(json_data, content_type="application/json")

    print(f"Data ingested and saved to GCS: gs://{bucket_name}/{file_path}")

def ingest_channel_youtube_data(video_url, bucket_name, file_name):
    #
    yt = YouTube(video_url)
    channel_url=yt.channel_url
    ch=Channel(channel_url)
    channel_id=ch.channel_id
    channel_url=ch.channel_url
    channel_name=ch.channel_name
    channel_description=ch.initial_data["metadata"]['channelMetadataRenderer']['description']
    channel_keywords=ch.initial_data["metadata"]['channelMetadataRenderer']['keywords']
    channel_subcribed=ch.initial_data["header"]["c4TabbedHeaderRenderer"]["subscriberCountText"]['simpleText']
    # Create a dictionary to store the data
    data = {
        "ChannelID": channel_id,
        "url": channel_url,
        "Name": channel_name,
        "description": channel_description,
        "subcribed": channel_subcribed,
        "keywords": channel_keywords
    }

    # Convert the data to JSON
    json_data = json.dumps(data)

    # Create a GCS client

    # Get the GCS bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Define the GCS file path
    file_path = f"Channel/{file_name}.json"

    # Create a GCS blob
    blob = bucket.blob(file_path)

    # Set the content type of the blob
    blob.content_type = "application/json"

    # Upload the JSON data to GCS
    blob.upload_from_string(json_data, content_type="application/json")

    print(f"Data ingested and saved to GCS: gs://{bucket_name}/{file_path}")

def ingest_caption_youtube_data(video_url, bucket_name, file_name):
    yt = YouTube(video_url)
    video_id=yt.video_id
    count=0
    for caption_track in yt.caption_tracks:
        count+=1
        language=caption_track.name
        content=yt.captions[caption_track.code].json_captions

        # Create a dictionary to store the data
        data = {
            "VideoID": video_id,
            "Language": language,
            "Contents": content
        }
        # Convert the data to JSON
        json_data = json.dumps(data)
        
        # Get the GCS bucket
        bucket = storage_client.get_bucket(bucket_name)

        # Define the GCS file path
        file_path = f"Caption/{file_name}_{str(count)}.json"

        # Create a GCS blob
        blob = bucket.blob(file_path)

        # Set the content type of the blob
        blob.content_type = "application/json"

        # Upload the JSON data to GCS
        blob.upload_from_string(json_data, content_type="application/json")

        print(f"Data ingested and saved to GCS: gs://{bucket_name}/{file_path}")
def ingest_thumnail_for_video_youtube_data(video_url, bucket_name, file_name):
    # Type(avata,banner,mobile_banner)
    # Get the YouTube video data
    yt = YouTube(video_url)
    video_id=yt.video_id
    channel_id=None
    thumbnail_url=None
    thumbnail_width=None
    thumbnail_height=None
    count =0
    thumbnail_type="Video"
    for thumbnail in yt.vid_info["videoDetails"][ 'thumbnail']['thumbnails']:
        count+=1
        thumbnail_url=thumbnail["url"]
        thumbnail_width=thumbnail["width"]
        thumbnail_height=thumbnail["height"]

        data = {
            "VideoID": video_id,
            "ChannelID": channel_id,
            "Type": thumbnail_type,
            "URL": thumbnail_url,
            "Width": thumbnail_width,
            "Height": thumbnail_height
        }
        # Convert the data to JSON
        json_data = json.dumps(data)
        
        # Get the GCS bucket
        bucket = storage_client.get_bucket(bucket_name)

        # Define the GCS file path
        file_path = f"Thumbnail/{file_name}_{str(count)}.json"

        # Create a GCS blob
        blob = bucket.blob(file_path)

        # Set the content type of the blob
        blob.content_type = "application/json"

        # Upload the JSON data to GCS
        blob.upload_from_string(json_data, content_type="application/json")

        print(f"Data ingested and saved to GCS: gs://{bucket_name}/{file_path}")
    return count
def ingest_thumnail_for_channel_youtube_data(video_url, bucket_name, file_name,thumbnail_type,thumbnail_id):
    # Type(avata,banner,mobile_banner)
    # Get the YouTube video data
    yt = YouTube(video_url)
    channel_url=yt.channel_url
    channel=Channel(channel_url)
    count=thumbnail_id
    for thumbnail in channel.initial_data["header"]['c4TabbedHeaderRenderer'][thumbnail_type]['thumbnails']:
        count+=1
        video_id=None
        channel_id=channel.channel_id
        thumbnail_url= thumbnail["url"]
        thumbnail_width=thumbnail["width"]
        thumbnail_height=thumbnail["height"]
        data = {
            "VideoID": video_id,
            "ChannelID": channel_id,
            "Type": thumbnail_type,
            "URL": thumbnail_url,
            "Width": thumbnail_width,
            "Height": thumbnail_height
        }
        # Convert the data to JSON
        json_data = json.dumps(data)
        
        # Get the GCS bucket
        bucket = storage_client.get_bucket(bucket_name)

        # Define the GCS file path
        file_path = f"Thumbnail/{file_name}_{str(count)}.json"

        # Create a GCS blob
        blob = bucket.blob(file_path)

        # Set the content type of the blob
        blob.content_type = "application/json"

        # Upload the JSON data to GCS
        blob.upload_from_string(json_data, content_type="application/json")

        print(f"Data ingested and saved to GCS: gs://{bucket_name}/{file_path}")

    return count
# Example usage
video_id= 'ZtBzWUZbTvA'
video_url = f'https://www.youtube.com/watch?v={video_id}'
bucket_name = "video_storage_yt"
file_name = video_id


ingest_video_youtube_data(video_url, bucket_name, file_name)
ingest_channel_youtube_data(video_url, bucket_name, file_name)
ingest_caption_youtube_data(video_url, bucket_name, file_name)
thumbnail_id=ingest_thumnail_for_video_youtube_data(video_url, bucket_name, file_name)
thumbnail_id=ingest_thumnail_for_channel_youtube_data(video_url, bucket_name, file_name, "avatar", thumbnail_id)
thumbnail_id=ingest_thumnail_for_channel_youtube_data(video_url, bucket_name, file_name, "banner", thumbnail_id)
thumbnail_id=ingest_thumnail_for_channel_youtube_data(video_url, bucket_name, file_name, "tvBanner", thumbnail_id)

