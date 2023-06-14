from google.cloud import bigquery
from google.cloud import storage
from plugins.helpers.utils import *
# Thiết lập thông tin xác thực
# Đảm bảo đã cài đặt và cấu hình gcloud SDK trên máy tính của bạn

# Thiết lập client BigQuery
bq_client = bigquery.Client()

# Thiết lập client GCS
gcs_client = storage.Client()

# Tên bucket và đường dẫn đến file trên GCS
bucket_name = 'my-gcs-bucket'
file_path = 'path/to/my/file.csv'

# Tên dataset và bảng trong BigQuery
dataset_id = 'my-bigquery-dataset'
table_id = 'my-bigquery-table'

# Đọc dữ liệu từ file trên GCS
bucket = gcs_client.get_bucket(bucket_name)
blob = bucket.blob(file_path)
data = blob.download_as_text()

# Tạo một đối tượng BigQuery DatasetReference
dataset_ref = bq_client.dataset(dataset_id)

# Tạo một đối tượng BigQuery TableReference
table_ref = dataset_ref.table(table_id)

# Tạo một đối tượng BigQuery LoadJobConfig
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1
job_config.autodetect = True

# Tạo job để load dữ liệu vào BigQuery
load_job = bq_client.load_table_from_file(data, table_ref, job_config=job_config)

# Chờ cho job hoàn thành
load_job.result()

# Kiểm tra kết quả
if load_job.state == 'DONE':
    print('Dữ liệu đã được lưu vào BigQuery thành công.')
else:
    print('Có lỗi xảy ra trong quá trình lưu dữ liệu vào BigQuery.')
