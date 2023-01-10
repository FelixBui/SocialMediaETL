import abc

import tqdm
from dateutil import tz
import pandas as pd
import pymongo
from core.app_base import AppBase
from libs.storage_utils import load_sa_creds
from bson.objectid import ObjectId


local_tz = tz.tzlocal()
utc_tz = tz.tzutc()
tqdm.tqdm.pandas()


class MgBase(AppBase):

    def __init__(self, config):
        super(MgBase, self).__init__(config)
        self.dp_ops = self.get_param_config(['dp-ops'])
        self.bq_creds = load_sa_creds(self.dp_ops)
        mongo_uri = self.get_param_config(['db', 'mg-competitor', 'uri'])
        db_name = self.get_param_config(['mongo_db'])
        self.col = self.get_param_config(['mongo_col'])
        self.src_db = pymongo.MongoClient(mongo_uri)[db_name]
        self.query_template = self.get_param_config(['query_template'])
        self.bq_tbl = self.get_param_config(['bq_tbl'])
        self.col_map = {}  # self.get_param_config(['col_map'])

    def extract(self):
        query = self.query_template
        from_id = ObjectId.from_datetime(self.from_date)
        to_id = ObjectId.from_datetime(self.to_date)
        query.update({"_id": {"$gte": from_id, "$lt": to_id}})

        df_raw = pd.DataFrame(list(
            self.src_db[self.col].find(
                query, self.col_map.keys()
            )))
        return df_raw

    def execute(self):
        raw_df = self.extract()
        df = self.transform(raw_df)
        self.load(df)

    @abc.abstractmethod
    def transform(self, raw_df):
        pass
        # df = raw_df.rename(columns=self.col_map)
        # df = df.drop(columns=['_id'])
        # return df.fillna(0)

    @abc.abstractmethod
    def load(self, df):
        pass
        # df.to_gbq(self.bq_tbl, if_exists="replace", credentials=self.bq_creds)
