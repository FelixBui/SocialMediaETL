from core.app_base import AppBase
import pandas as pd
from dateutil import tz

from libs.storage_utils import load_df_from_mongo, insert_df_to_postgres, load_df_from_postgres

local_tz = tz.tzlocal()


class TaskCentralizeTopdev(AppBase):

    job_cols = [
        '_id', 'job_id', 'objectID', 'company_id',
        'job_title', 'job_url', 'job_description', 'job_requirement',
        'areas', 'experience', 'level',
        'job_type', 'skills', 'website', 'address',
        'job_fields', 'tech_stack', 'nationality', 'benefits',
        'is_red', 'is_background', 'is_hot',
        'job_updated_at', 'created_at', 'updated_at']

    mapping_job = {
        'job_id': 'id',
        'job_title': 'name',
        'job_url': 'url',
        'job_description': 'description',
        'job_tags': 'tags',
        'job_requirement': 'requirement',
        'job_benefits': 'benefits'
    }

    map_size_name = {
        0: "Không xác định",
        1: "Dưới 20 người",
        2: "20 - 150 người",
        3: "150 - 300 người",
        4: "Trên 300 người"
    }

    def __init__(self, config):
        super(TaskCentralizeTopdev, self).__init__(config)
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
        df_raw = load_df_from_mongo(self.mongo_conf, collection_name="topdev", query=query, no_id=False)
        df_raw['created_at'] = df_raw['_id'].map(lambda x: x.generation_time.astimezone(local_tz))

        return df_raw

    def transform(self, raw_df):
        raw_df['company_id'] = raw_df['company_id'].astype(str)
        raw_df['job_id'] = raw_df['job_id'].astype(str)

        for col in self.job_cols:
            if col not in raw_df.columns:
                raw_df[col] = None

        df_job = raw_df[self.job_cols].rename(columns=self.mapping_job)
        df_job['address'] = df_job['address'].map(lambda x: [x] if isinstance(x, str) else x)

        return df_job

    def load(self, job_df):
        job_df['_id'] = job_df['_id'].map(str)
        insert_df_to_postgres(
            self.postgres_conf, tbl_name="topdev_job",
            df=job_df, primary_keys=["id"])

    def execute(self):
        self.log.info("step extract")
        raw_df = self.extract()
        self.log.info("step transform")
        job_df = self.transform(raw_df)
        self.log.info("step load")
        self.load(job_df)
