from core.app_base import AppBase
import pandas as pd
from dateutil import tz
from dateutil.parser import parse as parse_time

from libs.storage_utils import load_df_from_mongo, insert_df_to_postgres

local_tz = tz.tzlocal()


class TaskCentralizeCl(AppBase):
    job_cols = [
        '_id', 'id', 'objectID', 'name', 'url', 'company_id', 'areas', 'salary',
        'position', 'position_type', 'career_level',
        'experience_level', 'education_level', 'gender', 'job_fields',
        'job_description', 'job_requirement', 'contact_name', 'posted_at',
        'expired_at', 'job_street_address',
        'job_district', 'job_province', 'job_country', 'job_tags', 'age', 'job_code',
        'is_tlp_job', 'is_hot_job', 'is_red_job',
        'job_updated_date', 'created_at', 'updated_at', 'posted_at', 'expired_at'
    ]

    company_cols = [
        'company_id', 'company_name', 'company_url',
        'company_size', 'company_profile', 'created_at', 'updated_at'
    ]

    mapping_job = {
        "job_description": "description",
        "job_requirement": "requirement",
        "job_street_address": "street_address",
        "job_fields": "fields",
        "job_province": "province",
        "job_district": "district",
        "job_country": "country",
        "job_tags": "tags",
        "job_code": "code"
    }

    mapping_company = {
        'company_id': 'id',
        'company_name': 'name',
        'company_profile': 'profile',
        'company_url': 'url',
        'company_size': 'size'
    }

    map_size_name = {
        0: "Không xác định",
        1: "Dưới 20 người",
        2: "20 - 150 người",
        3: "150 - 300 người",
        4: "Trên 300 người"
    }

    def __init__(self, config):
        super(TaskCentralizeCl, self).__init__(config)
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

    @staticmethod
    def parse_company_size(x):
        if not(isinstance(x, str)):
            return None
        else:
            x = x.replace('employees', '').replace('Fewer than ', '0 - ').replace(' ', '').replace('.', '')
            return x

    def extract(self):
        query = {
            "updated_at": {
                "$gte": self.from_date,
                "$lt": self.to_date
            }
        }
        df_raw = load_df_from_mongo(self.mongo_conf, collection_name="cl", query=query, no_id=False)
        return df_raw

    def transform(self, df_raw):
        df_raw['created_at'] = df_raw['_id'].map(lambda x: x.generation_time.astimezone(local_tz))
        df_raw['contact_name'] = df_raw['contact_name'].str.strip()
        df_raw['company_size'] = df_raw['company_size'].str.strip()
        df_raw['job_province'] = df_raw['job_province'].str.strip()
        df_raw['job_district'] = df_raw['job_district'].str.strip()
        df_raw['job_country'] = df_raw['job_country'].str.strip()
        df_raw['contact_name'] = df_raw['contact_name'].str.strip()
        df_raw['expired_at'] = df_raw['expired_at'].str.strip().map(parse_time)
        df_raw['posted_at'] = df_raw['posted_at'].str.strip().map(parse_time)

        df_raw['company_id'] = df_raw['company_id'].astype(str)
        df_raw['id'] = df_raw['id'].astype(str)

        for col in self.job_cols:
            if col not in df_raw.columns:
                df_raw[col] = None
        for col in self.company_cols:
            if col not in df_raw.columns:
                df_raw[col] = None

        df_job = df_raw[self.job_cols].rename(columns=self.mapping_job).drop_duplicates(['id'])
        df_job['_id'] = df_job['_id'].map(str)
        df_company = df_raw[self.company_cols].rename(columns=self.mapping_company).drop_duplicates(['id'])

        # norm size
        df_company['size'] = df_company['size'].map(lambda x: self.parse_company_size(x))
        df_company['size_min'] = df_company['size'].map(lambda x:  int(x.split('-')[0]) if isinstance(x, str) else None)
        df_company['size_max'] = df_company['size'].map(lambda x:  int(x.split('-')[1]) if isinstance(x, str) else None)
        df_company['size_code'] = df_company['size_max'].map(self.norm_size)
        df_company['size_name'] = df_company['size_code'].map(self.map_size_name)

        # name
        df_company['name'] = df_company['name'].str.strip().str.lower()
        df_company['channel_code'] = 'CL'

        # tax code
        df_company_taxid = load_df_from_mongo(
            self.mongo_conf, 'company_lookup_result',
            query={"search_rank": 1}, selected_keys=["original_name", "taxid"]).rename(
            columns={"taxid": "tax_code"})
        df_company_taxid['tax_code'] = df_company_taxid['tax_code'].map(lambda x: x.split('-')[0])

        df_company = df_company.join(df_company_taxid.set_index('original_name'), 'name')

        return df_job, df_company

    def load(self, df_job, df_company):
        insert_df_to_postgres(self.postgres_conf, tbl_name="cl_job",
                              df=df_job, primary_keys=['id'])
        insert_df_to_postgres(self.postgres_conf, tbl_name="cl_company",
                              df=df_company.drop_duplicates(['id']), primary_keys=['id'])

    def execute(self):
        self.log.info("step extract")
        df_raw = self.extract()
        self.log.info("step transform")
        df_job, df_company = self.transform(df_raw)
        self.log.info("step load")
        self.load(df_job, df_company)


if __name__ == '__main__':
    import datetime
    import json
    config = {
        "process_name": "test",
        "execution_date": datetime.datetime.now(),
        "from_date": datetime.datetime(2021, 1, 1),
        "to_date": datetime.datetime.now(),
        "params": {
            "db": json.load(open("/Users/thucpk/IdeaProjects/crawlab/etl/config/default/db.json")),
            "telegram": json.load(open("/Users/thucpk/IdeaProjects/crawlab/etl/config/default/telegram.json")),
        }
    }
    task = TaskCentralizeCl(config)
    task.execute()
