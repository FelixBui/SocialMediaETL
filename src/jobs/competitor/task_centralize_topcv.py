import pandas as pd
import datetime
import json
import requests
import re
from dateutil import tz

from core.app_base import AppBase
from libs.storage_utils import load_df_from_mongo, insert_df_to_postgres
from consolidated.normalize import norm_areas

local_tz = tz.tzlocal()


class TaskCentralizeTopcv(AppBase):

    map_size_name = {
        0: "Không xác định",
        1: "Dưới 20 người",
        2: "20 - 150 người",
        3: "150 - 300 người",
        4: "Trên 300 người"
    }

    def __init__(self, config):
        super(TaskCentralizeTopcv, self).__init__(config)
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
            "error": {"$exists": False}
        }
        df_raw_topcv = load_df_from_mongo(self.mongo_conf, collection_name="topcv",
                                          query=query, no_id=False)
        df_raw_topcv['created_at'] = df_raw_topcv['_id'].map(lambda x: x.generation_time.astimezone(local_tz))
        df_raw_meta = self.extract_meta()
        return df_raw_topcv, df_raw_meta

    def extract_meta(self):
        df_topcv_meta_cur = load_df_from_mongo(
            mongo_conf=self.mongo_conf,
            collection_name="topcv",
            query={"$or": [{"is_highlight": True}, {"is_red": True}]},
            selected_keys=["id", "is_highlight", "is_red"]
        ).rename(columns={
            "is_highlight": "is_job_highlight"
        })

        df_best_job = pd.DataFrame(
            requests.get("https://www.topcv.vn/api-featured-jobs?limit=1000").json()['jobs'])

        df_high_salary_job = pd.DataFrame(
            requests.get("https://www.topcv.vn/ajax-high-salary-jobs?limit=1000").json()['jobs'])

        df_attractive_job = pd.DataFrame(
            requests.get("https://www.topcv.vn/ajax-attractive-jobs?limit=1000").json()['jobs'])

        df_hot_job = pd.DataFrame(
            requests.get("https://www.topcv.vn/ajax-hot-jobs?limit=1000").json()['jobs'])

        df_parttime_job = pd.DataFrame(
            requests.get("https://www.topcv.vn/ajax-parttime-jobs?limit=1000").json()['jobs'])

        df_intern_job = pd.DataFrame(
            requests.get("https://www.topcv.vn/ajax-intern-jobs?limit=1000").json()['jobs'])

        df_new_job = pd.DataFrame(
            requests.get("https://www.topcv.vn/ajax-new-jobs?limit=1000").json()['jobs'])

        df_topcv_home = pd.concat([
            df_best_job, df_high_salary_job, df_attractive_job, df_hot_job,
            df_parttime_job, df_intern_job, df_new_job]).drop_duplicates(['id'])

        df_best_job['is_best'] = True
        df_high_salary_job['is_high_salary'] = True
        df_attractive_job['is_attractive'] = True
        df_parttime_job['is_parttime'] = True
        df_intern_job['is_intern'] = True
        df_extra = df_best_job[['id', 'is_best']].join(
            df_high_salary_job[['id', 'is_high_salary']].set_index(['id']), 'id', 'outer'
        ).join(
            df_attractive_job[['id', 'is_attractive']].set_index(['id']), 'id', 'outer'
        ).join(
            df_parttime_job[['id', 'is_parttime']].set_index(['id']), 'id', 'outer'
        ).join(
            df_intern_job[['id', 'is_intern']].set_index(['id']), 'id', 'outer'
        )

        df_topcv_meta_cur['id'] = df_topcv_meta_cur['id'].map(str)
        df_extra['id'] = df_extra['id'].map(str)
        df_topcv_home['id'] = df_topcv_home['id'].map(str)

        return df_topcv_home.join(
            df_extra.join(
                df_topcv_meta_cur.set_index(['id']),
                'id',
                'outer'
            ).fillna(False).set_index(["id"]),
            "id",
            "outer"
        ).fillna(False)[[
            'id', 'is_featured', 'is_highlight', 'is_urgent', 'is_new',
            'is_paid_featured', 'is_hot', 'is_bg_featured', 'is_remote',
            'is_best', 'is_high_salary', 'is_attractive', 'is_parttime',
            'is_intern', 'is_job_highlight', 'is_red'
        ]]

    @staticmethod
    def get_topcv_size_min_max(x, pos=0):
        if x is None:
            return None
        if isinstance(x, str):
            x = x.strip().replace('10000+', '10000-999999').replace(
                '1000+', '1000-4999').replace('5000+', '5000-9999').split('-')
            if len(x) > 1:
                return int(x[pos])

    def transform(self, df_raw_topcv, df_raw_meta):
        # job
        df_raw_topcv['id'] = df_raw_topcv['id'].map(str)
        df_topcv_job = df_raw_topcv.rename(columns={
            "tags": "job_fields",
            "deadline": "expired_at"
        }).loc[:, [
            'objectID', 'id', '_id', 'name', 'company_id', 'url',
            'updated_at', 'created_at', 'expired_at',
            'salary', 'areas', 'job_updated_at', 'sourcetype',
            'job_fields', 'description', 'type', 'info',
            'skills', 'company_address'
        ]]
        df_topcv_job_info = pd.DataFrame(df_topcv_job['info'].map(self.parse_job_info_topcv).tolist()).rename(
            columns={
                "Số lượng cần tuyển": "slots",
                "Yêu cầu kinh nghiệm": "experience",
                "Hình thức làm việc": "job_type",
                "Giới tính": "sex",
                "Chức vụ": "level",
                "Địa điểm làm việc": "address",
                "Cấp bậc": "level",
                "Mức lương": "salary_range",
                "Số lượng tuyển": "slots",
                "Kinh nghiệm": "experience"
            }
        )[['slots', 'experience', 'job_type', 'sex', 'level']]
        df_topcv_job = df_topcv_job.join(df_topcv_job_info).rename(columns={
            "company_address": "address"
        })
        df_topcv_job['description'] = df_topcv_job['description'].map(
            lambda x: [['mô tả công việc', x]] if isinstance(x, str) else x
        )
        df_topcv_job['description'] = df_topcv_job['description'].map(dict).map(
            lambda x: json.dumps(x,  ensure_ascii=False))
        df_topcv_job['raw_description'] = df_topcv_job['description'].str.lower().map(lambda x: json.loads(x))
        df_topcv_job['description'] = df_topcv_job['raw_description'].map(lambda x: x.get('mô tả công việc'))
        df_topcv_job['requirement'] = df_topcv_job['raw_description'].map(lambda x: x.get('yêu cầu ứng viên'))
        df_topcv_job['raw_description'] = df_topcv_job['raw_description'].map(
            lambda x: json.dumps(x,  ensure_ascii=False)
        )
        df_topcv_job['info'] = df_topcv_job['info'].map(dict).map(lambda x: json.dumps(x,  ensure_ascii=False))

        # fill level na
        df_topcv_job.loc[
            df_topcv_job['level'].isna(), 'level'
        ] = df_topcv_job.loc[
            df_topcv_job['level'].isna(), 'name'
        ].map(lambda x: self.fill_level_na(str(x).lower()))

        # areas
        df_topcv_job['areas'] = df_topcv_job['areas'].map(norm_areas)

        dt_max = datetime.datetime(2100, 1, 1, tzinfo=local_tz)
        df_topcv_job['expired_at'] = df_topcv_job['expired_at'].map(
            lambda x: dt_max
            if isinstance(x, datetime.datetime) and x > dt_max
            else x)

        # topcv meta
        df_raw_meta['id'] = df_raw_meta['id'].map(str)
        df_raw_meta = df_raw_meta.rename(columns={
            "id": "job_id"
        })
        str_time = self.execution_date.strftime("%Y%m%d%H_")
        df_raw_meta["id"] = df_raw_meta["job_id"].map(lambda x: str_time + x)
        cur_at = datetime.datetime.now().replace(tzinfo=local_tz)
        df_raw_meta['updated_at'] = cur_at
        df_topcv_meta = df_raw_meta[['id', 'job_id', 'updated_at',
                                     'is_featured', 'is_highlight', 'is_urgent', 'is_new',
                                     'is_paid_featured', 'is_hot', 'is_bg_featured', 'is_remote',
                                     'is_best', 'is_high_salary', 'is_attractive', 'is_parttime',
                                     'is_intern', 'is_job_highlight', 'is_red']]

        return df_topcv_job, df_topcv_meta

    def load(self, job_df, meta_df):
        job_df['_id'] = job_df['_id'].map(str)
        insert_df_to_postgres(self.postgres_conf, tbl_name="topcv_job",
                              df=job_df, primary_keys=['id'])
        insert_df_to_postgres(self.postgres_conf, tbl_name="topcv_meta",
                              df=meta_df, primary_keys=["id"])

    def execute(self):
        self.log.info("step extract")
        df_raw_topcv, df_raw_meta = self.extract()
        self.log.info("step transform")
        df_topcv_job, df_topcv_meta = self.transform(df_raw_topcv, df_raw_meta)
        self.log.info("step load")
        self.load(df_topcv_job, df_topcv_meta)
