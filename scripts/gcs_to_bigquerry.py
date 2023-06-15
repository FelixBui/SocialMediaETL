from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import json

credentials = service_account.Credentials.from_service_account_file('C:/Users/Admin/Downloads/sa-key-SMETL.json')
storage_client = storage.Client(credentials=credentials)
# Thiết lập thông tin xác thực
# Đảm bảo đã cài đặt và cấu hình gcloud SDK trên máy tính của bạn

# Thiết lập client BigQuery


# Thiết lập client GCS


# Tên bucket và đường dẫn đến thư mục chứa các file JSON trên GCS
project_id = "socialmediaetl-386712"
folder_path = 'Metadata/Thumbnail/2023-6-14/'
bucket_name = 'video_storage_yt'

# Tên dataset và bảng trong BigQuery
dataset_id = 'socialmediaetl_dwh'
table_id = 'Metadata_thumbnail_demo'

# Lấy đối tượng bucket trên GCS
bucket = storage_client.get_bucket(bucket_name)

# Lấy danh sách các đối tượng Blob trong thư mục
blobs = bucket.list_blobs(prefix=folder_path)

bq_client = bigquery.Client(credentials=credentials,project=project_id)
# Duyệt qua từng file JSON
for blob in blobs:
    # Kiểm tra nếu là file JSON
    if blob.name.endswith('.json'):
        # Đọc nội dung của file JSON
        uri=f"gs://video_storage_yt/{blob.name}"

        # Tạo đối tượng BigQuery DatasetReference
        dataset_ref = bq_client.dataset(dataset_id)

        # Tạo đối tượng BigQuery TableReference
        table_ref = dataset_ref.table(table_id)

        # Tạo đối tượng BigQuery LoadJobConfig
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("ThumbnailID","STRING"),
                bigquery.SchemaField("VideoID","STRING","NULLABLE"),
                bigquery.SchemaField("URL","STRING","NULLABLE"),
                bigquery.SchemaField("Width","INTEGER","NULLABLE"),
                bigquery.SchemaField("Height","INTEGER","NULLABLE"),
                bigquery.SchemaField("Timestamp","FLOAT","NULLABLE")
            ],
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND"
        )
        
        # Tạo job để load dữ liệu vào BigQuery
        load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)

        # Chờ cho job hoàn thành
        
        load_job.result()
        # Kiểm tra kết quả
        if load_job.state == 'DONE':
            print(f"Dữ liệu từ file {blob.name} đã được lưu vào BigQuery thành công.")
        else:
            print(f"Có lỗi xảy ra trong quá trình lưu dữ liệu từ file {blob.name} vào BigQuery.")
        

