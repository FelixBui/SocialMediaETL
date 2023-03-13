import pandas as pd
import datetime
from core.app_base import AppBase
from dateutil import tz
import numpy as np

from libs.storage_utils import load_df_from_postgres, insert_df_to_postgres


local_tz = tz.tzlocal()


class TaskPricingVnw(AppBase):

    def __init__(self, config):
        super(TaskPricingVnw, self).__init__(config)
        self.postgres_conf = self.get_param_config(['db', 'pg-competitor'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

    @staticmethod
    def get_discount(x, mapping_discount, bins, names):
        return mapping_discount[pd.cut([x], bins, labels=names)[0]]

    def cal_vnw_company_pricing(self, x, sigma=0.9):
        mapping_vnw_pricing = {
            "num_jobs": 623.500,
            "is_mobile_hot_job": 3270.150,
            "is_mobile_top_job": 4237.650,
            "is_bold_and_red_job": 537.500,
            "is_headhunt_job": 3639.735,
            "is_top_management_job": 3639.735,
            "is_urgent_job": 3639.735,
            "is_premium": 8690.085,
            "is_home_featured": 8690.085
        }
        mapping_vnw_discount = {
            '1-2': 1,
            '3-4': 0.9,
            '5-9': 0.85,
            '10-20': 0.8,
            '20+': 0.7
        }
        bins = [-np.inf, 2, 4, 9, 19, np.inf]
        names = ['1-2', '3-4', '5-9', '10-20', '20+']

        is_mobile_hot_job = x['is_mobile_hot_job']
        is_mobile_top_job = x['is_mobile_top_job']
        is_bold_and_red_job = x['is_bold_and_red_job']
        is_headhunt_job = x['is_headhunt_job']
        is_top_management_job = x['is_top_management_job']
        is_urgent_job = x['is_urgent_job']
        is_premium = x['is_premium']
        is_home_featured = x['is_home_featured']
        num_jobs = x['num_jobs']

        is_mobile_hot_job_mtd = x['is_mobile_hot_job_mtd']
        is_mobile_top_job_mtd = x['is_mobile_top_job_mtd']
        is_bold_and_red_job_mtd = x['is_bold_and_red_job_mtd']
        is_headhunt_job_mtd = x['is_headhunt_job_mtd']
        is_top_management_job_mtd = x['is_top_management_job_mtd']
        is_urgent_job_mtd = x['is_urgent_job_mtd']
        is_premium_mtd = x['is_premium_mtd']
        is_home_featured_mtd = x['is_home_featured_mtd']
        num_jobs_mtd = x['num_jobs_mtd']

        sale_price = (
                num_jobs * mapping_vnw_pricing['num_jobs'] *
                self.get_discount(num_jobs_mtd, mapping_vnw_discount, bins, names)
                + is_mobile_hot_job * mapping_vnw_pricing['is_mobile_hot_job'] *
                self.get_discount(is_mobile_hot_job_mtd, mapping_vnw_discount, bins, names)
                + is_mobile_top_job * mapping_vnw_pricing['is_mobile_top_job'] *
                self.get_discount(is_mobile_top_job_mtd, mapping_vnw_discount, bins, names)
                + is_bold_and_red_job * mapping_vnw_pricing['is_bold_and_red_job'] *
                self.get_discount(is_bold_and_red_job_mtd, mapping_vnw_discount, bins, names)
                + is_headhunt_job * mapping_vnw_pricing['is_headhunt_job'] *
                self.get_discount(is_headhunt_job_mtd, mapping_vnw_discount, bins, names)
                + is_top_management_job * mapping_vnw_pricing['is_top_management_job'] *
                self.get_discount(is_top_management_job_mtd, mapping_vnw_discount, bins, names)
                + is_urgent_job * mapping_vnw_pricing['is_urgent_job'] *
                self.get_discount(is_urgent_job_mtd, mapping_vnw_discount, bins, names)
                + is_premium * mapping_vnw_pricing['is_premium'] *
                self.get_discount(is_premium_mtd, mapping_vnw_discount, bins, names)
                + is_home_featured * mapping_vnw_pricing['is_home_featured'] *
                self.get_discount(is_home_featured_mtd, mapping_vnw_discount, bins, names)
        )

        original_price = (
                num_jobs * mapping_vnw_pricing['num_jobs']
                + is_mobile_hot_job * mapping_vnw_pricing['is_mobile_hot_job']
                + is_mobile_top_job * mapping_vnw_pricing['is_mobile_top_job']
                + is_bold_and_red_job * mapping_vnw_pricing['is_bold_and_red_job']
                + is_headhunt_job * mapping_vnw_pricing['is_headhunt_job']
                + is_top_management_job * mapping_vnw_pricing['is_top_management_job']
                + is_urgent_job * mapping_vnw_pricing['is_urgent_job']
                + is_premium * mapping_vnw_pricing['is_premium']
                + is_home_featured * mapping_vnw_pricing['is_home_featured']
        )

        x['sale_price'] = sale_price * sigma
        x['original_price'] = original_price * sigma
        return x

    def extract(self):
        df_vnw_meta = load_df_from_postgres(
            self.postgres_conf,
            query="""
            select * from vnw_meta
            """)

        df_vnw_job_company = load_df_from_postgres(
            self.postgres_conf,
            query="""
            select 
              id, 
              company_id, 
              name, 
              date_trunc('day', created_at) as interval_time,
              date_trunc('month', created_at) as interval_month
            from vnw_job
            """)

        return df_vnw_meta, df_vnw_job_company

    def transform(self, df_vnw_meta, df_vnw_job_company):
        df_vnw_meta['created_at'] = df_vnw_meta['id'].map(
            lambda x: datetime.datetime.strptime(x.split('_')[0], "%Y%m%d"))

        df_vnw_job_meta = df_vnw_meta.groupby(["job_id", ])[
            'is_premium', 'is_urgent_job', 'is_mobile_hot_job', 'is_mobile_top_job',
            'is_bold_and_red_job', 'is_top_management_job', 'is_headhunt_job', 'is_home_featured'
        ].agg(any).applymap(lambda x: 1 if x else 0).reset_index()
        df_vnw_company_joined = df_vnw_job_company.loc[~df_vnw_job_company['company_id'].isin(['192082', '751'])].join(
            df_vnw_job_meta.set_index('job_id'),
            'id'
        ).fillna(0)

        df_vnw_company_meta_month = df_vnw_company_joined.groupby(['company_id', 'interval_month']).agg({
            'is_premium': sum, 'is_urgent_job': sum, 'is_mobile_hot_job': sum,
            'is_mobile_top_job': sum, 'is_bold_and_red_job': sum,
            'is_top_management_job': sum, 'is_headhunt_job': sum, 'is_home_featured': sum,
            'id': len
        }).reset_index().rename(
            columns={
                "id": "num_jobs_mtd",
                "is_premium": "is_premium_mtd",
                "is_urgent_job": "is_urgent_job_mtd",
                "is_mobile_hot_job": "is_mobile_hot_job_mtd",
                "is_mobile_top_job": "is_mobile_top_job_mtd",
                "is_bold_and_red_job": "is_bold_and_red_job_mtd",
                "is_top_management_job": "is_top_management_job_mtd",
                "is_headhunt_job": "is_headhunt_job_mtd",
                "is_home_featured": "is_home_featured_mtd"
            }
        )

        df_vnw_company_meta_day = df_vnw_company_joined.groupby([
            'company_id', 'interval_month', 'interval_time'
        ]).agg({
            'is_premium': sum, 'is_urgent_job': sum, 'is_mobile_hot_job': sum,
            'is_mobile_top_job': sum, 'is_bold_and_red_job': sum,
            'is_top_management_job': sum, 'is_headhunt_job': sum, 'is_home_featured': sum,
            'id': len
        }).sort_values(['id']).reset_index().rename(columns={"id": "num_jobs"})

        df_vnw_company_meta = df_vnw_company_meta_day.join(
            df_vnw_company_meta_month.set_index(['company_id',  "interval_month"]),
            ['company_id',  "interval_month"]
        )
        # names = ['<5', '5-10', '10-20', '20-40', '40+']
        # df_vnw_company_meta['group'] = pd.cut(df_vnw_company_meta['num_jobs_mtd'],
        #                                       bins=[0, 5, 10, 20, 40, np.inf], labels=names)
        df_vnw_company_meta = df_vnw_company_meta.apply(self.cal_vnw_company_pricing, axis=1)

        return df_vnw_company_meta

    def load(self, df_vnw_company_meta):
        insert_df_to_postgres(
            self.postgres_conf, tbl_name="vnw_company_pricing",
            df=df_vnw_company_meta, primary_keys=["company_id", "interval_time"])

    def execute(self):
        self.log.info("step extract")
        df_vnw_meta, df_vnw_job_company = self.extract()
        self.log.info("step transform")
        df_vnw_company_meta = self.transform(
            df_vnw_meta, df_vnw_job_company)
        self.log.info("step load")
        self.load(df_vnw_company_meta)
