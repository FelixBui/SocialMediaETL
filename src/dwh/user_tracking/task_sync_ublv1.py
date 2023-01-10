import datetime
import pymongo

import pandas as pd
from bson.objectid import ObjectId
from dwh.user_tracking.user_tracking_base import UserTrackingBase
import json
import pytz
from libs.storage_utils import upload_df_to_gcs, get_gcs_cli

sg_tz = pytz.timezone("Asia/Saigon")
utc_tz = pytz.utc


class TaskSyncUblv1(UserTrackingBase):

    def __init__(self, config):
        super(TaskSyncUblv1, self).__init__(config)
        dp_ops = self.get_param_config(['dp-ops'])
        mongo_uri = self.get_param_config(['db', 'ublv1', 'uri'])
        self.tracking_tool_db = pymongo.MongoClient(mongo_uri)['tracking_tool']
        self.gcs_cli = get_gcs_cli(dp_ops)
        self.bucket_name = self.get_param_config(['bucket_name'])
        self.from_date = self.from_date.replace(minute=0, second=0, microsecond=0).astimezone(sg_tz)
        self.to_date = self.to_date.replace(minute=0, second=0, microsecond=0).astimezone(sg_tz)

    def load_ubl_from_raw(self, from_time, to_time):
        from_id = ObjectId.from_datetime(from_time)
        to_id = ObjectId.from_datetime(to_time)
        df_raw = pd.DataFrame(list(
            self.tracking_tool_db['log'].find(
                {"_id": {"$gte": from_id, "$lt": to_id}},
                ['created_at_ts', 'raw', 'trackingId', 'type']
            )))
        return df_raw

    def execute(self):

        self.log.info("#step 1: extract data")
        df_raw = self.load_ubl_from_raw(self.from_date, self.to_date)

        self.log.info("#step 2: transform")
        df_raw['created_at'] = df_raw['created_at_ts'].map(
            lambda x: datetime.datetime.fromtimestamp(x).replace(tzinfo=sg_tz).astimezone(utc_tz))
        df_raw['local_created_at'] = df_raw['created_at'] + datetime.timedelta(hours=7)
        df_raw = df_raw.drop(columns=['created_at_ts'])
        df_raw['_id'] = df_raw['_id'].map(lambda x: str(x))
        df_raw["raw"] = df_raw["raw"].map(lambda x: json.dumps(x))

        # step 3: upload gcs
        self.log.info("#step 3: load")
        gcs_path = "user_tracking/ublv1/{}".format(
            self.from_date.strftime("%Y-%m-%d/%H%M.pq")
        )
        upload_df_to_gcs(self.gcs_cli, df_raw, self.bucket_name, gcs_path)
