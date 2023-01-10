import pandas as pd
import datetime
from core.app_base import AppBase
from dateutil import tz
import numpy as np

from libs.storage_utils import load_df_from_postgres, insert_df_to_postgres


local_tz = tz.tzlocal()


class TaskPricingCb(AppBase):

    def __init__(self, config):
        super(TaskPricingCb, self).__init__(config)
        self.postgres_conf = self.get_param_config(['db', 'pg-competitor'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

    @staticmethod
    def get_discount(x, mapping_discount, bins, names):
        return mapping_discount[pd.cut([x], bins, labels=names)[0]]

    def cal_cb_company_pricing(self, x, sigma=0.9):
        mapping_cb_pricing = {
            "num_jobs": 2450,
            "is_unlimited_1": 25102 / 12,
            "is_unlimited_2": 13596 / 6,
            "is_unlimited_3": 8500 / 3,
            "is_highlight": 2450,
            "is_hot_job": 5230,
            "is_vip_job": 7730,
            "is_premium": 8700,
        }
        mapping_cb_discount = {
            '1-2': 1,
            '3-4': 0.9,
            '5-9': 0.85,
            '10-20': 0.8,
            '20+': 0.7
        }
        bins = [-np.inf, 2, 4, 9, 19, np.inf]
        names = ['1-2', '3-4', '5-9', '10-20', '20+']

        rs = 0
        is_highlight = x['is_highlight']
        is_premium = x['is_premium']
        is_hot_job = x['is_hot_job']
        is_vip_job = x['is_vip_job']
        # num_jobs = x['num_jobs']
        level_unlimited = x['level_unlimited']
        if level_unlimited == 'is_unlimited_1':
            num_jobs = 0
            is_vip_job = 0
            is_hot_job = max(is_hot_job - 6, 0)
            is_highlight = max(is_hot_job - 7, 0)
            rs += mapping_cb_pricing[level_unlimited]
        elif level_unlimited == 'is_unlimited_2':
            num_jobs = 0
            is_vip_job = 0
            is_hot_job = max(is_hot_job - 2, 0)
            is_highlight = max(is_hot_job - 4, 0)
            rs += mapping_cb_pricing[level_unlimited]
        elif level_unlimited == 'is_unlimited_3':
            num_jobs = 0
            is_vip_job = 0
            is_hot_job = max(is_hot_job - 2, 0)
            is_highlight = max(is_highlight - 1, 0)
            rs += mapping_cb_pricing[level_unlimited]
        else:
            num_jobs = 1

        if is_premium:
            is_highlight = max(is_highlight - 1, 0)

        if is_vip_job:
            is_highlight = max(is_highlight - 1, 0)

        if is_hot_job:
            is_highlight = max(is_highlight - 1, 0)

        rs += (
                num_jobs * mapping_cb_pricing['num_jobs']
                + is_vip_job * mapping_cb_pricing['is_vip_job'] *
                self.get_discount(is_vip_job, mapping_cb_discount, bins, names)
                + is_hot_job * mapping_cb_pricing['is_hot_job'] *
                self.get_discount(is_hot_job, mapping_cb_discount, bins, names)
                + is_highlight * mapping_cb_pricing['is_highlight'] *
                self.get_discount(is_highlight, mapping_cb_discount, bins, names)
                + is_premium * mapping_cb_pricing['is_premium'] *
                self.get_discount(is_premium, mapping_cb_discount, bins, names)
        )
        return rs * sigma

    def extract(self):

        df_cb_meta = load_df_from_postgres(
            self.postgres_conf,
            query="""
        select * from cb_meta
        """)

        df_cb_job_company = load_df_from_postgres(
            self.postgres_conf,
            query="""
            select job.id as id, job.name as name, to_char(job.created_at, 'YYYY-MM') as interval_time, 
            company.id as company_id, company.name as company_name
            from cb_job job
            join cb_company company
            on company.id = job.company_id
            """)

        return df_cb_meta, df_cb_job_company

    def transform(self, df_cb_meta, df_cb_job_company):
        df_cb_meta['created_at'] = df_cb_meta['id'].map(
            lambda x: datetime.datetime.strptime(x.split('_')[0], "%Y%m%d%H"))

        df_cb_job_meta = df_cb_meta.groupby(["job_id"])[
            'is_highlight', 'is_premium', 'is_red',
            'is_hot_job', 'is_vip_job'
        ].agg(any).applymap(lambda x: 1 if x else 0).reset_index()

        df_cb_company_meta = df_cb_job_company.loc[df_cb_job_company['company_id'] != '35A4E9A9'].join(
            df_cb_job_meta.set_index('job_id'),
            'id'
        ).fillna(0).groupby(['company_id', 'interval_time']).agg({
            'is_highlight': sum, 'is_premium': sum, 'is_red': sum,
            'is_hot_job': sum, 'is_vip_job': sum, 'id': len
        }).sort_values(['id']).reset_index().rename(columns={"id": "num_jobs"})
        names = ['<5', '5-10', '10-20', '20-40', '40+']
        df_cb_company_meta['group'] = pd.cut(
            df_cb_company_meta['num_jobs'], bins=[0, 5, 10, 20, 40, np.inf], labels=names)
        names = ["on_demand", 'is_unlimited_3', 'is_unlimited_2', 'is_unlimited_1']
        df_cb_company_meta['level_unlimited'] = pd.cut(
            df_cb_company_meta['num_jobs'], bins=[0, 10, 20, 40, np.inf], labels=names)
        df_cb_company_meta['price'] = df_cb_company_meta.apply(self.cal_cb_company_pricing, axis=1)

        return df_cb_company_meta

    def load(self, df_cb_company_meta):
        insert_df_to_postgres(
            self.postgres_conf, tbl_name="cb_company_pricing",
            df=df_cb_company_meta, primary_keys=["company_id", "interval_time"])

    def execute(self):
        self.log.info("step extract")
        df_cb_meta, df_cb_job_company = self.extract()
        self.log.info("step transform")
        df_cb_company_meta = self.transform(df_cb_meta, df_cb_job_company)
        self.log.info("step load")
        self.load(df_cb_company_meta)
