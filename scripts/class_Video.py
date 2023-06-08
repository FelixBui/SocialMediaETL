
class Video(youtube):

    def __init__(self, video_url: str, sa_key: str, video_path: str, bucket_name: Optrional[str]):
        super.__init__(self,video_url,sa_key, bucket_name)
        self.video_path=video_path

    def extract(self):
        youtube.streams.get_by_itag(22).download(output_path=video_path,filename=file_name)

    def transform(self) -> list:
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
                print(description)
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

    def load(self, data_list: list, folder: Optional = "Video", type_data: Optional = "application/json"):
        super.load(data_list,folder,type_data)

    def load_video_gcs(self, type_data: str, file_path: str):
        blob = bucket.blob(file_path)

        blob.content_type = type_data

        # Upload the JSON data to GCS
        blob.upload_from_filename(video_path, content_type=type_data)

        print(f"Data ingested and saved to GCS: gs://{bucket_name}/{file_path}")

    

