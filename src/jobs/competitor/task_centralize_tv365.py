import datetime

from core.app_base import AppBase
import pandas as pd
from dateutil import tz

from libs.storage_utils import load_df_from_mongo, insert_df_to_postgres

local_tz = tz.tzlocal()


class TaskCentralizeTv365(AppBase):
    job_cols = [
        '_id', 'job_id', 'objectID', 'company_id', 'title', 'job_url',
        'provinces', 'salary', 'job_level', 'experience', 'job_type', 'degree', 'sex',
        'number_requirement', 'commission', 'probationary_period',
        'job_fields', 'job_tags', 'province', 'distinct', 'job_description',
        'job_requirement', 'job_benefits', 'job_brief', 'job_view',
        'expired_at', 'job_expired_at', 'job_post_at', 'created_at', 'updated_at']

    mapping_job = {
        'job_id': 'id',
        'job_url': 'url',
        'job_level': 'level',
        'job_tags': 'tags',
        'job_description': 'description',
        'job_requirement': 'requirement',
        'job_benefits': 'benefits',
        'job_post_at': 'job_updated_at'
    }

    map_size_name = {
        0: "Không xác định",
        1: "Dưới 20 người",
        2: "20 - 150 người",
        3: "150 - 300 người",
        4: "Trên 300 người"
    }

    def __init__(self, config):
        super(TaskCentralizeTv365, self).__init__(config)
        self.mongo_conf = self.get_param_config(['db', 'mg-competitor'])
        self.postgres_conf = self.get_param_config(['db', 'pg-competitor'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

    @staticmethod
    def norm_size(x_max):
        range_size = [20, 150, 300]
        if (x_max is None) or pd.isna(x_max):
            return 0
        for ix, v in enumerate(range_size):
            if x_max < v:
                return ix + 1
        else:
            return len(range_size) + 1

    def extract(self):
        query = {
            "updated_at": {
                "$gte": self.from_date,
                "$lt": self.to_date
            }
        }
        df_raw = load_df_from_mongo(self.mongo_conf, collection_name="tv365", query=query, no_id=False)
        df_raw['created_at'] = df_raw['_id'].map(lambda x: x.generation_time.astimezone(local_tz))

        return df_raw

    def transform(self, df_raw):
        df_raw['company_id'] = df_raw['company_id'].astype(str)
        df_raw['job_id'] = df_raw['job_id'].astype(str)

        for col in self.job_cols:
            if col not in df_raw.columns:
                df_raw[col] = None

        df_job = df_raw[self.job_cols].rename(columns=self.mapping_job)
        df_job['job_expired_at'] = df_job['job_expired_at'].map(
            lambda x: datetime.datetime.strptime(x, '%d/%m/%Y').replace(tzinfo=local_tz))
        df_job['job_updated_at'] = df_job['job_updated_at'].map(
            lambda x: datetime.datetime.strptime(x, '%d/%m/%Y').replace(tzinfo=local_tz))

        return df_job

    def load(self, df_job):
        df_job['_id'] = df_job['_id'].map(str)
        insert_df_to_postgres(self.postgres_conf, tbl_name="tv365_job",
                              df=df_job, primary_keys=['id'])

    def execute(self):
        self.log.info("step extract")
        raw_df = self.extract()
        self.log.info("step transform")
        job_df = self.transform(raw_df)
        self.log.info("step load")
        self.load(job_df)
