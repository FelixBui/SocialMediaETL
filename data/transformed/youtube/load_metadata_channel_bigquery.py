from data.upload.load_metadata_bigquery import *
from configs.variables_1 import *
from google.cloud import bigquery

class Load_Metadata_Channel_Bigquery(Load_Metadata_Bigquery):
    def __init__(self):
        super().__init__()
    def extract(self):
        pass 
    def transform(self):
        job_config = bigquery.LoadJobConfig(
            schema=[
            bigquery.SchemaField("ChannelID","STRING","NULLABLE"),
                bigquery.SchemaField("URL","STRING","NULLABLE"),
                bigquery.SchemaField("Name","STRING","NULLABLE"),
                bigquery.SchemaField("Description","STRING","NULLABLE"),
                bigquery.SchemaField("Subcribed","STRING","NULLABLE"),
                bigquery.SchemaField("Keywords","STRING","NULLABLE"),
                bigquery.SchemaField("Timestamp","FLOAT","NULLABLE")
            ],
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND"
        )
        return job_config
    def load(self,job_config,folder: str = folder["fd_channel"],table_id: str = TABLE_ID["Channel"]):
        super().load(job_config,folder,table_id)
    def execute(self):
        job_config = self.transform()
        self.load(job_config)