import pandas as pd
from core.app_base import AppBase
from dateutil import tz
import numpy as np

from libs.storage_utils import load_df_from_postgres, insert_df_to_postgres


local_tz = tz.tzlocal()


class TaskPricingItviec(AppBase):

    def __init__(self, config):
        super(TaskPricingItviec, self).__init__(config)
        self.postgres_conf = self.get_param_config(['db', 'pg-competitor'])
        # self.from_date = self.get_process_info(['from_date'])
        # self.to_date = self.get_process_info(['to_date'])

    @staticmethod
    def get_discount(x, mapping_discount, bins, names):
        return mapping_discount[pd.cut([x], bins, labels=names)[0]]

    def cal_company_pricing(self, x, sigma=1):
        mapping_pricing = {
            "is_regular_job": 2298,
            "is_hot_job": 5067,
        }

        mapping_itv_discount = {
            '1-2': 1,
            '3-4': 0.9,
            '5-9': 0.85,
            '10-20': 0.8,
            '20+': 0.75
        }
        bins = [-np.inf, 2, 4, 9, 19, np.inf]
        names = ['1-2', '3-4', '5-9', '10-20', '20+']

        is_regular_job = x['is_regular_job']
        is_hot_job = x['is_hot_job']
        num_jobs = is_regular_job + is_hot_job
        num_jobs_mtd = x['num_jobs_mtd']
        original_price = is_regular_job * mapping_pricing['is_regular_job'] \
            + is_hot_job * mapping_pricing['is_hot_job'] * sigma
        sale_price = self.get_discount(num_jobs_mtd, mapping_itv_discount, bins, names) * original_price * sigma
        x['num_jobs'] = num_jobs
        x['original_price'] = original_price
        x['sale_price'] = sale_price
        return x

    def extract(self):
        df_job_company = load_df_from_postgres(
            self.postgres_conf,
            query="""
            select 
              id,
              company_id, 
              class_job,
              date_trunc('day', created_at) as interval_time,
              date_trunc('month', created_at) as interval_month,
              count(*) OVER (
                 PARTITION BY company_id, date_trunc('month', created_at)
              ) AS num_jobs_mtd
            from itv_job
        """)
        return df_job_company

    def transform(self, df_job_company):
        df_company_meta = df_job_company.groupby(
            ['company_id', 'interval_month', 'interval_time', 'class_job', 'num_jobs_mtd']
        )['id'].count().reset_index()
        df_company_meta['is_regular_job'] = df_company_meta.apply(
            lambda x: x['id'] if x['class_job'] == 'job' else 0, axis=1)
        df_company_meta['is_hot_job'] = df_company_meta.apply(
            lambda x: x['id'] if x['class_job'] != 'job' else 0, axis=1)
        df_company_meta = df_company_meta.groupby(
            ['company_id', 'interval_month', 'interval_time', 'num_jobs_mtd']
        )[['is_regular_job', 'is_hot_job']].sum().reset_index()
        df_company_meta = df_company_meta.apply(self.cal_company_pricing, axis=1)
        return df_company_meta

    def load(self, df_company_meta):
        insert_df_to_postgres(
            self.postgres_conf, tbl_name="itv_company_pricing",
            df=df_company_meta, primary_keys=["company_id", "interval_time"])

    def execute(self):
        self.log.info("step extract")
        df_job_company = self.extract()
        self.log.info("step transform")
        df_company_meta = self.transform(
            df_job_company)
        self.log.info("step load")
        self.load(df_company_meta)
