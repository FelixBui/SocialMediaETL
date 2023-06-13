import os
from data.raw.ingestion_base import Ingestion,



class Ingest_TikTok(Ingestion):
    def __init__(self, source: str):
        super().__init__(source)
        self.bucket = get_gcs_bucket()
        self._airflow_home = os.environ['AIRFLOW_HOME']

    def extract(self):
        return super().extract()
    
    def transform(self):
        return super().transform()

    def load(self, source_file_path: str, destination_blob_name: str, content_type: str):
        return super().load(source_file_path, destination_blob_name, content_type)        

    def execute(self):
        return super().execute()
