class Thumbnail(YT):

    def __init__(self, video_url: str , sa_key: str, bucket_name: Optional [str] ):
        super.__init__(self, video_url, sa_key, bucket_name)

    def extract(self):
        pass

    def transform(self):
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

            data = {
                "ThumbnailID": thumbnail_id,
                "VideoID": video_id,
                "URL": thumbnail_url,
                "Width": thumbnail_width,
                "Height": thumbnail_height
            }

            data_list.append(data)
        return data_list

    def load(self, data_list: list, folder: Optional = "Thumbnail", type_data: Optional = "application/json"):
        super.load(data_list,folder,type_data)



