from class_SMETL import SMETL
from typing import Optional
class YT(SMETL):

    def __init__(self, video_url: str, sa_key: str, bucket_name: Optional[str]):
        super.__init__(self,video_url,sa_key, bucket_name)
        self.youtube=YouTube(video_url)
        self.channel=Channel(youtube.channel_url)
        self.file_name=youtube.video_id

    def extract(self):
        pass
    
    def transform(self):
        pass

    def load(self, data_list: list, folder: str, type_data: str):
        count=0
        for data in data_list:
            count+=1
            json_data = json.dumps(data)
            # Get the GCS bucket
            file_path = f"{folder}/{file_name}_{str(count)}.json"
            # Create a GCS blob
            blob = bucket.blob(file_path)

            # Set the content type of the blob
            blob.content_type = type_data

            # Upload the JSON data to GCS
            blob.upload_from_string(json_data, content_type=type_data)

            print(f"Data ingested and saved to GCS: gs://{bucket_name}/{file_path}")



"""
    Code ẩu, không import thư viện vào thì dùng class con dùng kiểu gì ??? thừa kế của thằng cha nhưng không import thằng cha vào để thừa kế ??
"""