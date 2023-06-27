from data.load.load_metadata_bigquery import *
from configs.variables_1 import *
from google.cloud import bigquery

class Load_Metadata_Caption_Bigquery(Load_Metadata_Bigquery):
    def __init__(self):
        super().__init__()
    def extract(self):
        pass 
    def transform(self):
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("Contents_caption_id","STRING","NULLABLE"),
                bigquery.SchemaField("VideoID","STRING"),
                bigquery.SchemaField("Language","STRING","NULLABLE"),
                bigquery.SchemaField("Contents","JSON","NULLABLE"),
                bigquery.SchemaField("Timestamp","FLOAT","NULLABLE")
            ],
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND"
        )
        return job_config
    def load(self,job_config,folder: str = folder["fd_caption"],table_id: str = TABLE_ID["Caption"]):
        super().load(job_config,folder,table_id)
    def execute(self):
        job_config = self.transform()
        self.load(job_config)