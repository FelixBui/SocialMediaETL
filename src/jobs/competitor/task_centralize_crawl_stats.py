import tqdm
from dateutil import tz
import datetime
from dwh.mg.mg_base import MgBase


local_tz = tz.tzlocal()
utc_tz = tz.tzutc()
tqdm.tqdm.pandas()


class TaskCentralizeCrawlStats(MgBase):

    def __init__(self, config):
        super(TaskCentralizeCrawlStats, self).__init__(config)
        self.from_date = datetime.datetime(2020, 1, 1)
        self.to_date = datetime.datetime(2025, 1, 1)
        self.query_template = self.get_param_config(['query_template'])
        self.bq_tbl = self.get_param_config(['bq_tbl'])
        self.col_map = {
            'task_id': 'task_id',
            'start_time': 'start_time',
            'downloader/request_bytes': 'request_bytes',
            'downloader/response_bytes': 'response_bytes',
            'downloader/response_status_count/200': 'response_200_count',
            'finish_time': 'finish_time',
            'elapsed_time_seconds': 'elapsed_time_seconds',
            'spider_name': 'spider_name',
            'log_count/ERROR': 'error_count',
            'downloader/response_status_count/204': 'response_204_count',
            'downloader/response_status_count/307': 'response_307_count',
            'downloader/response_status_count/302': 'response_302_count',
            'downloader/response_status_count/404': 'response_404_count',
            'downloader/response_status_count/301': 'response_301_count',
            'downloader/response_status_count/500': 'response_500_count',
            'downloader/response_status_count/403': 'response_403_count',
            'downloader/response_status_count/524': 'response_524_count',
            'downloader/response_status_count/503': 'response_503_count'
        }

    def execute(self):
        raw_df = self.extract()
        df = self.transform(raw_df)
        self.load(df)

    def transform(self, raw_df):
        df = raw_df.rename(columns=self.col_map)
        df = df.drop(columns=['_id'])
        return df.fillna(0)

    def load(self, df):
        df.to_gbq(self.bq_tbl, if_exists="replace", credentials=self.bq_creds)
