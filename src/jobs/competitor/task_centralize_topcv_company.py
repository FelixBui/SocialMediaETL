import pandas as pd
import datetime

import re
from dateutil import tz

from core.app_base import AppBase
from libs.storage_utils import load_df_from_mongo, insert_df_to_postgres
from consolidated.normalize import norm_website

local_tz = tz.tzlocal()


class TaskCentralizeTopcvCompany(AppBase):

    map_size_name = {
        0: "Không xác định",
        1: "Dưới 20 người",
        2: "20 - 150 người",
        3: "150 - 300 người",
        4: "Trên 300 người"
    }
    company_cols = [
        'company_id', 'company_url', 'company_name',
        'updated_at', 'created_at',
        'objectID', 'company_address', 'type', 'company'
    ]

    def __init__(self, config):
        super(TaskCentralizeTopcvCompany, self).__init__(config)
        self.mongo_conf = self.get_param_config(['db', 'mg-competitor'])
        self.postgres_conf = self.get_param_config(['db', 'pg-competitor'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

    @staticmethod
    def parse_job_info_topcv(x):
        rs = dict()
        for k, v in x:
            k = k.replace(":", "").strip()
            rs[k] = v
        return rs

    @staticmethod
    def parse_str_updated_topcv(x, current_at):
        pattern_time = re.compile(r"(\d+) (\S+) trước")
        delta_time, delta_interval = pattern_time.search(x).groups()
        delta_time = int(delta_time)
        delta_seconds = {
            "giây": delta_time,
            "phút": delta_time * 60,
            "giờ": delta_time * 3600,
            "ngày": delta_time * 24 * 3600,
            "tuần": delta_time * 24 * 3600 * 7,
            "tháng": delta_time * 24 * 3600 * 30,
            "năm": delta_time * 24 * 3600 * 365
        }.get(delta_interval)
        job_updated_at = current_at - datetime.timedelta(seconds=delta_seconds)
        return job_updated_at

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
    def fill_level_na(x):
        mapping_topcv_level = {
            "nhân viên": "Nhân viên",
            "trưởng nhóm": "Trưởng nhóm",
            "cộng tác viên": "Thực tập sinh",
            "ctv": "Thực tập sinh",
            "chuyên viên": "Nhân viên",
            "quản lý": "Quản lý / Giám sát",
            "management": "Quản lý / Giám sát",
            "tts": "Thực tập sinh",
            "thực tập sinh": "Thực tập sinh",
            "giám đốc": "Giám đốc",
            "manager": "Trưởng nhóm",
            "trưởng phòng": "Trưởng/Phó phòng",
            "teamleader": "Trưởng nhóm",
            "team leader": "Trưởng nhóm",
            "lead": "Quản lý / Giám sát",
            "leader": "Trưởng nhóm",
            "officer": "Giám đốc",
            "developer": "Nhân viên",
            "chuyên gia": "Quản lý / Giám sát",
            "solution architect": "Quản lý / Giám sát",
            "executive": "Nhân viên",
            "senior": "Nhân viên",
            "engineer": "Nhân viên",
            "internship": "Thực tập sinh",
            "lập trình viên": "Nhân viên",
            "giám sát": "Quản lý / Giám sát",
            "trưởng": "Trưởng nhóm",
        }
        level_pattern = re.compile(
            r"(nhân viên)|(cộng tác viên)|(chuyên viên)|(quản lý)|(giám sát)|"
            "(trưởng nhóm)|(management)|(manager)|(team leader)|(leader)|(lead)|(trưởng)|"
            "(giám đốc)|(trưởng phòng)|(teamleader)|(officer)|"
            "(developer)|(solution architect)|(executive)|(senior)|(engineer)|(lập trình viên)|"
            "(internship)|(ctv)|(thực tập sinh)|(tts)")
        match = level_pattern.search(x)
        level = "Nhân viên"
        if match:
            level = mapping_topcv_level.get(match.group(), "Nhân viên")
        return level

    def extract(self):
        query = {
            "updated_at": {
                "$gte": self.from_date,
                "$lt": self.to_date
            },
            "error": {"$exists": False},
        }
        raw_df = load_df_from_mongo(
            self.mongo_conf, collection_name="topcv", query=query,
            selected_keys=self.company_cols, no_id=False)
        raw_df['created_at'] = raw_df['_id'].map(lambda x: x.generation_time.astimezone(local_tz))
        return raw_df

    @staticmethod
    def get_topcv_size_min_max(x, pos=0):
        if x is None:
            return None
        if isinstance(x, str):
            x = x.strip().replace('10000+', '10000-999999').replace(
                '1000+', '1000-4999').replace('5000+', '5000-9999').split('-')
            if len(x) > 1:
                return int(x[pos])

    def transform(self, raw_df):
        # company
        company_df = raw_df.loc[:, self.company_cols].sort_values('created_at').drop_duplicates(['company_id'])

        if company_df.shape[0] > 0:
            company_info_df = pd.DataFrame(company_df['company'].values.tolist())[
                ['company_website', 'company_size', 'company_description']]
            company_df = company_df.join(company_info_df).rename(columns={
                "company_id": "id",
                "company_url": "url",
                "company_name": "name",
                "company_address": "address",
                "company_website": "website",
                "company_size": "size",
                "company_description": "description"
            }).drop(columns=['company'])
            company_df['id'] = company_df['id'].map(str)
            company_df['size'] = company_df['size'].map(
                lambda x: x.strip().replace("nhân viên", "") if isinstance(x, str) else x)
            company_df['is_brand'] = company_df['type'].map(lambda x: True if x == 'brand' else False)

            # size
            company_df['size'].map(lambda x: x.strip() if not pd.isna(x) else x)
            company_df['size_min'] = company_df['size'].map(lambda x: self.get_topcv_size_min_max(x, 0))
            company_df['size_max'] = company_df['size'].map(lambda x: self.get_topcv_size_min_max(x, 1))
            company_df['size_code'] = company_df['size_max'].map(self.norm_size)
            company_df['size_name'] = company_df['size_code'].map(self.map_size_name)

            # name
            company_df['name'] = company_df['name'].str.strip().str.lower()
            company_df['website'] = company_df['website'].map(norm_website)
            company_df['channel_code'] = 'TOPCV'

            # tax code
            company_taxid_df = load_df_from_mongo(
                self.mongo_conf, 'company_lookup_result',
                query={"search_rank": 1}, selected_keys=["original_name", "taxid"]).rename(
                columns={"taxid": "tax_code"})
            company_taxid_df['tax_code'] = company_taxid_df['tax_code'].map(lambda x: x.split('-')[0])
            company_df = company_df.join(company_taxid_df.set_index('original_name'), 'name')

        return company_df

    def load(self, company_df):
        insert_df_to_postgres(
            self.postgres_conf, tbl_name="topcv_company",
            df=company_df, primary_keys=['id'])

    def execute(self):
        self.log.info("step extract")
        raw_df = self.extract()
        self.log.info("step transform")
        company_df = self.transform(raw_df)
        self.log.info("step load")
        self.load(company_df)
