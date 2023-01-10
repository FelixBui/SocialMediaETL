from dateutil import tz

from core.app_base import AppBase
from libs.storage_utils import insert_df_to_postgres, load_df_from_mongo


local_tz = tz.tzlocal()


class TaskCentralizeCompanyStatus(AppBase):

    def __init__(self, config):
        super(TaskCentralizeCompanyStatus, self).__init__(config)
        self.mongo_conf = self.get_param_config(['db', 'mg-competitor'])
        self.postgres_conf = self.get_param_config(['db', 'pg-dp-services'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

    def extract(self):
        query = {
            "updated_at": {
                "$gte": self.from_date,
                "$lt": self.to_date
            }
        }
        df_raw = load_df_from_mongo(self.mongo_conf, collection_name="dkkd", query=query, no_id=False)
        df_raw['created_at'] = df_raw['_id'].map(lambda x: x.generation_time.astimezone(local_tz))
        df_raw = df_raw[[
            'tax_code',
            'company_name',
            'province',
            'status',
            'tax_updated_at',
            'created_at',
            'updated_at']]
        return df_raw

    def load(self, df_company_status):
        insert_df_to_postgres(
            self.postgres_conf,
            tbl_name="official_company_status",
            df=df_company_status.drop_duplicates(['tax_code']),
            primary_keys=['tax_code'])

    def execute(self):
        self.log.info("step extract")
        df_company_status = self.extract()
        self.log.info("step load")
        self.load(df_company_status)
