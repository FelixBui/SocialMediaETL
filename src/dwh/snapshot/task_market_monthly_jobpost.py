from core.app_base import AppBase
from libs.storage_utils import load_sa_creds, load_df_from_bq, get_gcs_cli, upload_df_to_gcs
import pytz
from datetime import timedelta
import pandas as pd

vn_tz = pytz.timezone('Asia/Saigon')

class TaskMarketMonthlyJobpost(AppBase):

    def __init__(self, config):
        super(TaskMarketMonthlyJobpost, self).__init__(config)
        self.dp_ops = self.get_param_config(['dp-ops'])
        self.from_date = self.get_process_info(["from_date"])
        self.to_date = self.from_date + timedelta(days= 1)
        self.to_date = self.get_process_info(["to_date"])
        self.gcs_path = self.get_param_config(['gsc_path'])
        self.bucket_name = self.get_param_config(['bucket_name'])
        self.query = self.get_param_config(['query'])
        self.bq_creds = load_sa_creds(self.dp_ops)

    def extract(self):
        df = load_df_from_bq(self.bq_creds, self.query)
        return df

    def transform(self, df):
        df['snapshot_dt'] = self.to_date
        df['snapshot_dt'] = pd.to_datetime(df['snapshot_dt']).dt.date
        return df

    def load(self, df):
        gcs_cli = get_gcs_cli(self.dp_ops)
        upload_df_to_gcs( gcs_cli, df, self.bucket_name,  self.gcs_path.format( self.to_date.astimezone(vn_tz).strftime("%Y%m%d"),"000" ), chunk_size=262144, fmt='parquet')

    def execute(self):
        df = self.extract()
        df = self.transform(df)      
        self.load(df)

