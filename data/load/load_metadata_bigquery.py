
from data.socialmediaetl_base import SocialMediaETL
from plugins.helpers.utils import *
import json
from configs.variables import *

class Load_Metadata_Bigquery(SocialMediaETL):
    def __init__(self):
        super().__init__()
        self.bucket = get_gcs_bucket()
        self.bq_client = get_bigquery_client()
        self.dataset_ref = get_bigquery_dataset()
        
    def extract(self):
        pass 
    def transform(self):
        pass 
    def load(self, job_config, folder: str, table_id: str):
        datetime=datetime_now()
        folder_path = f"Metadata/{folder}/{datetime}/"
        blobs = self.bucket.list_blobs(prefix=folder_path)
        for blob in blobs:
            # Kiểm tra nếu là file JSON
            if blob.name.endswith('.json'):
                # Đọc nội dung của file JSON
                uri=f"gs://{YTB_BUCKET_NAME}/{blob.name}"
                table_ref = self.dataset_ref.table(table_id)
                load_job = self.bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
                # Chờ cho job hoàn thành       
                load_job.result()
                # Kiểm tra kết quả
                if load_job.state == 'DONE':
                    print(f"Dữ liệu từ file {blob.name} đã được lưu vào BigQuery thành công.")
                else:
                    print(f"Có lỗi xảy ra trong quá trình lưu dữ liệu từ file {blob.name} vào BigQuery.")
        
    def execute(self):
        return super().execute()

