import pandas as pd
import json
from core.app_base import AppBase
from dateutil import tz

from libs.storage_utils import load_df_from_mongo, insert_df_to_postgres
from consolidated.normalize import norm_areas


local_tz = tz.tzlocal()


class TaskCentralizeCb(AppBase):

    map_size_name = {
        0: "Không xác định",
        1: "Dưới 20 người",
        2: "20 - 150 người",
        3: "150 - 300 người",
        4: "Trên 300 người"
    }

    def __init__(self, config):
        super(TaskCentralizeCb, self).__init__(config)
        self.mongo_conf = self.get_param_config(['db', 'mg-competitor'])
        self.postgres_conf = self.get_param_config(['db', 'pg-competitor'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

    def extract(self):
        query = {
            "updated_at": {
                "$gte": self.from_date,
                "$lt": self.to_date
            }
        }
        df_raw_cb = load_df_from_mongo(self.mongo_conf, collection_name="cb", query=query, no_id=False)
        df_raw_cb['created_at'] = df_raw_cb['_id'].map(lambda x: x.generation_time.astimezone(local_tz))
        return df_raw_cb

    @staticmethod
    def transform(df_raw_cb):
        df_cb_job = df_raw_cb.rename(columns={
            "job_updated_date": "job_updated_at",
            "deadline": "expired_at"
        }).loc[:, [
            '_id', 'id', 'objectID', 'company_id', 'name', 'url',
            'job_updated_at', 'updated_at', 'expired_at', 'created_at',
            'areas', 'salary', 'sourcetype',
            'type', 'welfare', 'description', 'info']
        ]

        df_cb_job_info = pd.DataFrame(df_cb_job['info'].map(
            lambda x: {k.replace(':', '').strip(): v for k, v in x}).tolist()).rename(columns={
                "Ngày cập nhật": "job_updated",
                "Ngành nghề": "job_fields",
                "Hình thức": "job_type",
                "Cấp bậc": "level",
                "Kinh nghiệm": "experience",
                "Nơi làm việc": "address",
                "Bằng cấp": "degree",
                "Thời gian làm việc": "work_time",
                "Độ tuổi": "ages",
                "Phụ cấp": "allowance",
                "Địa điểm": "location",
                "Giới tính": "sex",
                "Phụ cấp khác": "other_allowances"
            }
        )
        cols = ['job_updated', 'job_fields', 'job_type', 'experience',
                'level', 'degree', 'work_time', 'ages',
                'allowance', 'sex', 'other_allowances']
        for col in cols:
            if col not in df_cb_job_info.columns:
                df_cb_job_info[col] = None
        df_cb_job_info = df_cb_job_info[cols]
        df_cb_job = df_cb_job.join(df_cb_job_info)
        df_cb_job['description'] = df_cb_job['description'].map(dict).map(lambda x: json.dumps(x,  ensure_ascii=False))
        df_cb_job['raw_description'] = df_cb_job['description'].str.lower().map(lambda x: json.loads(x))
        df_cb_job['description'] = df_cb_job['raw_description'].map(lambda x: "\n".join(x.get('mô tả công việc', [])))
        df_cb_job['requirement'] = df_cb_job['raw_description'].map(lambda x: "\n".join(x.get('yêu cầu công việc', [])))
        df_cb_job['raw_description'] = df_cb_job['raw_description'].map(
            lambda x: json.dumps(x,  ensure_ascii=False)
        )

        df_cb_job['info'] = df_cb_job['info'].map(dict).map(lambda x: json.dumps(x,  ensure_ascii=False))
        # areas
        df_cb_job['areas'] = df_cb_job['areas'].map(norm_areas)

        return df_cb_job

    def load(self, df_cb_job):
        df_cb_job['_id'] = df_cb_job['_id'].map(str)
        insert_df_to_postgres(self.postgres_conf, tbl_name="cb_job",
                              df=df_cb_job, primary_keys=['id'])

    def execute(self):
        self.log.info("step extract")
        df_raw_cb = self.extract()
        self.log.info("step transform")
        df_cb_job = self.transform(df_raw_cb)
        self.log.info("step load")
        self.load(df_cb_job)
