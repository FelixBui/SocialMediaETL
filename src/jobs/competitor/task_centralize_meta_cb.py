from dateutil import tz
from bson.objectid import ObjectId

from core.app_base import AppBase
from libs.storage_utils import load_df_from_mongo, insert_df_to_postgres


local_tz = tz.tzlocal()


class TaskCentralizeMetaCb(AppBase):

    def __init__(self, config):
        super(TaskCentralizeMetaCb, self).__init__(config)
        self.mongo_conf = self.get_param_config(['db', 'mg-competitor'])
        self.postgres_conf = self.get_param_config(['db', 'pg-competitor'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

    def extract(self):
        dummy_id = ObjectId.from_datetime(self.from_date)
        df_cb_meta = load_df_from_mongo(
            self.mongo_conf, collection_name="cb_meta",
            query={"_id": {"$gte": dummy_id}}, no_id=False
        )
        df_cb_meta['created_at'] = df_cb_meta['_id'].map(lambda x: x.generation_time.astimezone(local_tz))
        # filter time
        # yesterday = datetime.datetime.now().astimezone(local_tz) - datetime.timedelta(1)
        # df_cb_meta = df_cb_meta.loc[df_cb_meta['created_at'] > yesterday]
        return df_cb_meta

    def transform(self, df_raw_meta):
        # cb meta
        df_raw_meta['id'] = df_raw_meta['id'].map(str)
        df_cb_meta = df_raw_meta.copy().rename(columns={
            "id": "job_id",
            "objectID": "id"
        })
        df_cb_meta["id"] = df_cb_meta['id'].str.replace("cb_meta_", "")
        return df_cb_meta[["id", "job_id", "is_highlight", "is_premium", "is_red",
                           "is_hot_job", "is_vip_job", "updated_at", "created_at"]]

    def load(self, df_cb_meta):
        insert_df_to_postgres(
            self.postgres_conf, tbl_name="cb_meta",
            df=df_cb_meta, primary_keys=['id'])

    def execute(self):
        self.log.info("step extract")
        df_raw_meta = self.extract()
        self.log.info("step transform")
        df_cb_meta = self.transform(df_raw_meta)
        self.log.info("step load")
        self.load(df_cb_meta)
