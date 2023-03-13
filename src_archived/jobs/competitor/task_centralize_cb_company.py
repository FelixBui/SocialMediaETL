import pandas as pd
import re
from core.app_base import AppBase
from dateutil import tz

from libs.storage_utils import load_df_from_mongo, insert_df_to_postgres
from consolidated.normalize import norm_website


local_tz = tz.tzlocal()


class TaskCentralizeCbCompany(AppBase):

    map_size_name = {
        0: "Không xác định",
        1: "Dưới 20 người",
        2: "20 - 150 người",
        3: "150 - 300 người",
        4: "Trên 300 người"
    }
    company_cols = [
        'company_id', 'company_name', 'company_url', 'company_size',
        'company_address', 'company_info', 'company_website',
        'updated_at', 'created_at'
    ]

    def __init__(self, config):
        super(TaskCentralizeCbCompany, self).__init__(config)
        self.mongo_conf = self.get_param_config(['db', 'mg-competitor'])
        self.postgres_conf = self.get_param_config(['db', 'pg-competitor'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

    @staticmethod
    def parse_cb_company_info(x):
        rs = list()
        for e in x:
            v = e.split(':')
            v = [e.strip() for e in v]
            if len(v) > 2:
                t = list()
                t.append(v[0])
                t.append("".join(v[1:]))
                rs.append(t)
            else:
                rs.append(v)

        return dict(rs)

    @staticmethod
    def get_cb_size_min_max(x, pos=0):
        if pd.isna(x):
            return None
        x = x.strip().replace('.', '').replace(',', '').replace(' ', '')
        pattern_v1 = re.compile(r'(\d+)\D(\d+)')
        s = pattern_v1.search(x)
        if s:
            return int(s.groups()[pos])
        pattern_v2 = re.compile(r'(\d+)')
        s = pattern_v2.search(x)
        if s:
            return int(s.group())

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
        df_raw_cb = load_df_from_mongo(
            self.mongo_conf, collection_name="cb", query=query,
            selected_keys=self.company_cols, no_id=False)
        df_raw_cb['created_at'] = df_raw_cb['_id'].map(lambda x: x.generation_time.astimezone(local_tz))
        return df_raw_cb

    def transform(self, df_raw_cb):

        # company
        cb_company_df = df_raw_cb.loc[~df_raw_cb['company_id'].isna(), [
            'company_id', 'company_name', 'company_url', 'company_size',
            'company_address', 'company_info', 'company_website',
            'updated_at', 'created_at'
        ]].sort_values('created_at').drop_duplicates(['company_id'])

        if cb_company_df.shape[0] > 0:
            cb_company_df = cb_company_df.rename(columns={
                "company_id": "id",
                "company_name": "name",
                "company_url": "url",
                "company_size": "size",
                "company_address": "address",
                "company_website": "website"
            })

            cb_company_info_df = pd.DataFrame(cb_company_df['company_info'].map(
                lambda x: self.parse_cb_company_info(x)).tolist()).rename(columns={
                    "Người liên hệ": "contact",
                    "Qui mô công ty": "size",
                    "Loại hình hoạt động": "company_type",
                    "Website": "website"})
            for col in ['contact', 'company_type']:
                if col not in cb_company_info_df.columns:
                    cb_company_info_df[col] = None
            cb_company_df = cb_company_df.join(cb_company_info_df[['contact', 'company_type']])
            cb_company_df['size'] = cb_company_df['size'].map(
                lambda x: x.replace("nhân viên", "").replace(
                    "Nhân viên tại Việt Nam", "").replace(
                    "CBNV", "").strip() if isinstance(x, str) else x)

            # size
            cb_company_df['size_min'] = cb_company_df['size'].map(lambda x: self.get_cb_size_min_max(x, 0))
            cb_company_df['size_max'] = cb_company_df['size'].map(lambda x: self.get_cb_size_min_max(x, 1))
            cb_company_df['size_code'] = cb_company_df['size_max'].map(self.norm_size)
            cb_company_df['size_name'] = cb_company_df['size_code'].map(self.map_size_name)

            # name
            cb_company_df['name'] = cb_company_df['name'].str.strip().str.lower()
            cb_company_df['address'] = cb_company_df['address'].str.strip()
            cb_company_df['website'] = cb_company_df['website'].map(norm_website)
            cb_company_df['channel_code'] = 'CB'

            # tax code
            company_taxid_df = load_df_from_mongo(
                self.mongo_conf, 'company_lookup_result',
                query={"search_rank": 1}, selected_keys=["original_name", "taxid"]).rename(
                columns={"taxid": "tax_code"})
            company_taxid_df['tax_code'] = company_taxid_df['tax_code'].map(lambda x: x.split('-')[0])
            cb_company_df = cb_company_df.join(company_taxid_df.set_index('original_name'), 'name')

        return cb_company_df

    def load(self, df_cb_company):
        insert_df_to_postgres(self.postgres_conf, tbl_name="cb_company",
                              df=df_cb_company, primary_keys=['id'])

    def execute(self):
        self.log.info("step extract")
        df_raw_cb = self.extract()
        self.log.info("step transform")
        df_cb_company = self.transform(df_raw_cb)
        self.log.info("step load")
        self.load(df_cb_company)
