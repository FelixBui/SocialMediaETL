import datetime
import pandas as pd
from core.app_base import AppBase
from dateutil import tz
import numpy as np

from libs.storage_utils import load_df_from_postgres, insert_df_to_postgres


local_tz = tz.tzlocal()


class TaskPricingTopcv(AppBase):

    def __init__(self, config):
        super(TaskPricingTopcv, self).__init__(config)
        self.postgres_conf = self.get_param_config(['db', 'pg-competitor'])
        # self.from_date = self.get_process_info(['from_date'])
        # self.to_date = self.get_process_info(['to_date'])

    @staticmethod
    def get_discount(x, mapping_discount, bins, names):
        return mapping_discount[pd.cut([x], bins, labels=names)[0]]

    def cal_topcv_company_pricing(self, x, sigma=1):
        mapping_topcv_pricing = {
            "is_best": 4950,
            "is_attractive": 3300,
            "is_high_salary": 3300,
            "is_job_highlight": 2200,
            "is_urgent": 550,
            "is_hot": 550,
            "is_red": 550
        }

        mapping_topcv_discount = {
            '2-': 1,
            '2-4': 0.9,
            '5-9': 0.85,
            '10+': 0.8,
        }
        bins = [-np.inf, 1, 4, 9, np.inf]
        names = ['2-', '2-4', '5-9', '10+']

        is_urgent = x['is_urgent']
        is_hot = x['is_hot']
        is_best = x['is_best']
        is_high_salary = x['is_high_salary']
        is_attractive = x['is_attractive']
        is_job_highlight = x['is_job_highlight']
        is_red = x['is_red']

        is_urgent_mtd = x['is_urgent_mtd']
        is_hot_mtd = x['is_hot_mtd']
        is_best_mtd = x['is_best_mtd']
        is_high_salary_mtd = x['is_high_salary_mtd']
        is_attractive_mtd = x['is_attractive_mtd']
        is_job_highlight_mtd = x['is_job_highlight_mtd']
        is_red_mtd = x['is_red_mtd']

        sale_price = (
                is_urgent * mapping_topcv_pricing['is_urgent'] *
                self.get_discount(is_urgent_mtd, mapping_topcv_discount, bins, names)
                + is_hot * mapping_topcv_pricing['is_hot'] *
                self.get_discount(is_hot_mtd, mapping_topcv_discount, bins, names)
                + is_best * mapping_topcv_pricing['is_best'] *
                self.get_discount(is_best_mtd, mapping_topcv_discount, bins, names)
                + is_high_salary * mapping_topcv_pricing['is_high_salary'] *
                self.get_discount(is_high_salary_mtd, mapping_topcv_discount, bins, names)
                + is_attractive * mapping_topcv_pricing['is_attractive'] *
                self.get_discount(is_attractive_mtd, mapping_topcv_discount, bins, names)
                + is_job_highlight * mapping_topcv_pricing['is_job_highlight'] *
                self.get_discount(is_job_highlight_mtd, mapping_topcv_discount, bins, names)
                + is_red * mapping_topcv_pricing['is_red'] *
                self.get_discount(is_red_mtd, mapping_topcv_discount, bins, names)
        )
        original_price = (
                is_urgent * mapping_topcv_pricing['is_urgent']
                + is_hot * mapping_topcv_pricing['is_hot']
                + is_best * mapping_topcv_pricing['is_best']
                + is_high_salary * mapping_topcv_pricing['is_high_salary']
                + is_attractive * mapping_topcv_pricing['is_attractive']
                + is_job_highlight * mapping_topcv_pricing['is_job_highlight']
                + is_red * mapping_topcv_pricing['is_red']
        )
        x['sale_price'] = sale_price * sigma
        x['original_price'] = original_price * sigma
        return x

    def extract(self):
        df_topcv_meta = load_df_from_postgres(
            self.postgres_conf,
            query="""
        select * from topcv_meta
        """)

        df_topcv_job_company = load_df_from_postgres(
            self.postgres_conf,
            query="""
        select 
          id, 
          company_id, 
          date_trunc('day', created_at) as interval_time,
          date_trunc('month', created_at) as interval_month
        from topcv_job
        """)
        return df_topcv_meta, df_topcv_job_company

    def transform(self, df_topcv_meta, df_topcv_job_company):
        df_topcv_meta['created_at'] = df_topcv_meta['id'].map(
            lambda x: datetime.datetime.strptime(x.split('_')[0], "%Y%m%d%H"))

        df_topcv_job_meta = df_topcv_meta.fillna(False).groupby(['job_id'])[
            'is_urgent', 'is_hot', 'is_best',
            'is_high_salary', 'is_attractive',
            'is_job_highlight', 'is_red',
        ].agg(any).applymap(lambda x: 1 if x else 0).reset_index()

        df_topcv_company_joined = df_topcv_job_company.join(
            df_topcv_job_meta.set_index('job_id'),
            'id'
        ).fillna(0)
        df_topcv_company_meta_month = df_topcv_company_joined.groupby([
            'company_id',  "interval_month",
        ]).agg({
            'is_urgent': sum, 'is_hot': sum, 'is_best': sum,
            'is_high_salary': sum, 'is_attractive': sum,
            'is_job_highlight': sum, 'is_red': sum, 'id': len
        }).reset_index().rename(
            columns={
                'is_urgent': 'is_urgent_mtd',
                'is_hot': 'is_hot_mtd',
                'is_best': 'is_best_mtd',
                'is_high_salary': 'is_high_salary_mtd',
                'is_attractive': 'is_attractive_mtd',
                'is_job_highlight': 'is_job_highlight_mtd',
                'is_red': 'is_red_mtd',
                "id": "num_jobs_mtd"
            }
        )
        df_topcv_company_meta_day = df_topcv_company_joined.groupby([
            'company_id', "interval_month", "interval_time"
        ]).agg({
            'is_urgent': sum, 'is_hot': sum, 'is_best': sum,
            'is_high_salary': sum, 'is_attractive': sum,
            'is_job_highlight': sum, 'is_red': sum, 'id': len
        }).reset_index().rename(columns={"id": "num_jobs"})
        df_topcv_company_meta = df_topcv_company_meta_day.join(
            df_topcv_company_meta_month.set_index(['company_id',  "interval_month"]),
            ['company_id',  "interval_month"]
        )
        df_topcv_company_meta = df_topcv_company_meta.apply(self.cal_topcv_company_pricing, axis=1)
        return df_topcv_company_meta

    def load(self, df_topcv_company_meta):
        insert_df_to_postgres(
            self.postgres_conf, tbl_name="topcv_company_pricing",
            df=df_topcv_company_meta, primary_keys=["company_id", "interval_time"])

    def execute(self):
        self.log.info("step extract")
        df_topcv_meta, df_topcv_job_company = self.extract()
        self.log.info("step transform")
        df_topcv_company_meta = self.transform(
            df_topcv_meta, df_topcv_job_company)
        self.log.info("step load")
        self.load(df_topcv_company_meta)


if __name__ == '__main__':
    import json

    conf = {
        "process_name": "test",
        "execution_date": datetime.datetime.now(),
        "params": {
            "telegram": json.load(open("/Users/thucpk/IdeaProjects/crawlab/etl/config/default/telegram.json")),
            "db": json.load(open("/Users/thucpk/IdeaProjects/crawlab/etl/config/default/db.json"))
        }
    }
    task = TaskPricingTopcv(conf)
    task.execute()
