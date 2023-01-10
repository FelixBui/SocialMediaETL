import pprint
import pymongo
import json
import multiprocessing as mp
import tqdm

import pandas as pd
import pytz
from user_agents import parse as parse_ua
from urllib.parse import urlsplit, parse_qs
from bson.objectid import ObjectId
from dwh.user_tracking.user_tracking_base import UserTrackingBase
from libs.storage_utils import get_gcs_cli, upload_df_to_gcs

sg_tz = pytz.timezone("Asia/Saigon")


class TaskSyncClickUblv2(UserTrackingBase):

    def __init__(self, config):
        super(TaskSyncClickUblv2, self).__init__(config)
        mongo_uri = self.get_param_config(['db', 'ublv2', 'uri'])
        self.ublv2 = pymongo.MongoClient(mongo_uri)['ublv2']
        self.from_date = self.from_date.replace(minute=0, second=0, microsecond=0)
        self.to_date = self.to_date.replace(minute=0, second=0, microsecond=0)
        dp_ops = self.get_param_config(['dp-ops'])
        self.gcs_cli = get_gcs_cli(dp_ops)
        self.bucket_name = self.get_param_config(['bucket_name'])

    def extract(self):
        from_id = ObjectId.from_datetime(self.from_date)
        to_id = ObjectId.from_datetime(self.to_date)

        q = {
            "_id": {
                "$gte": from_id,
                "$lt": to_id
            },
            "event": {"$in": ["click", "click_modal"]},
            "$or": [
                {"target.event_name": {"$exists": "true"}},
                {"attributes.eventName": {"$exists": "true"}},
                {"attributes.event_name": {"$exists": "true"}},
                {"attributes.buttonName": {"$exists": "true"}},
                {"target.box": {"$exists": "true"}}
            ]
        }
        print(self.from_date, self.to_date)
        print(q)
        df_raw = pd.DataFrame(list(
            self.ublv2['events'].find(q)))
        return df_raw

    @staticmethod
    def parse_user_agent(obj):
        user_agent = parse_ua(obj)
        return dict(zip([
            'browser_family', 'browser_version',
            'os_family', 'os_version',
            'device_family', 'device_brand',
            'device_model', 'is_pc', 'is_tablet',
            'is_mobile', 'is_bot'
        ], (user_agent.browser.family,
            user_agent.browser.version_string,
            user_agent.os.family,
            user_agent.os.version_string,
            user_agent.device.family,
            user_agent.device.brand,
            user_agent.device.model,
            user_agent.is_pc,
            user_agent.is_tablet,
            user_agent.is_mobile,
            user_agent.is_bot)))

    @staticmethod
    def parse_url(url):
        params = {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}
        path = urlsplit(url).path
        params['path_url'] = path
        return params

    @classmethod
    def parse_row(cls, x):
        try:
            x['context_id'] = x['context']['id']
            x['context_type'] = x['context']['type']
            x['context_data'] = x['context']['data']
            x['context'] = json.dumps(x['context'])
            x['url'] = x['context_data'].get('url')
            x['referrer'] = x['context_data'].get('referrer')

            if ('attributes' in x) and not pd.isna(x['attributes']):
                if 'mouse' in x['attributes']:
                    x['mouse_px'] = x['attributes']['mouse']['px']
                    x['mouse_py'] = x['attributes']['mouse']['py']
                    x['mouse_sx'] = x['attributes']['mouse']['sx']
                    x['mouse_sy'] = x['attributes']['mouse']['sy']
                    x['mouse_cx'] = x['attributes']['mouse']['cx']
                    x['mouse_cy'] = x['attributes']['mouse']['cy']

                x['attributes'] = json.dumps(x['attributes'])
            else:
                x['mouse_px'] = None
                x['mouse_py'] = None
                x['mouse_sx'] = None
                x['mouse_sy'] = None
                x['mouse_cx'] = None
                x['mouse_cy'] = None

                x['attributes'] = None

            if ('target' in x) and not pd.isna(x['target']):
                x['el'] = x['target'].get('el')
                x['target'] = json.dumps(x['target'])
            else:
                x['el'] = None
                x['target'] = '{}'

            x['device_id'] = x['device']['deviceId']
            x['device_at'] = x['device']['startAt']
            x['device_ua'] = x['device']['ua']
            x['device_sw'] = x['device']['sw']
            x['device_sh'] = x['device']['sh']
            x['device_sd'] = x['device']['sd']
            x['device_tz'] = x['device'].get('tz')
            x['device'] = json.dumps(x['device'])

            x['device_info'] = json.dumps(cls.parse_user_agent(x['device_ua']))
            x['source_info'] = json.dumps(cls.parse_url(x['url']))

            x['session_id'] = x['session']['sessionId']
            x['session_at'] = x['session']['startAt']
            x['globalDeviceId'] = x['session'].get('globalDeviceId', None)
            experiment = x['session'].get('experiment')
            if experiment is not None:
                x['exp_device_id'] = experiment.get('expDeviceId')
                x['exp_session_id'] = experiment.get('expSessionId')
            else:
                x['exp_device_id'] = None
                x['exp_session_id'] = None
            x['session_duration'] = x['session']['duration']
            x['session'] = json.dumps(x['session'])

            if pd.isna(x['tab']):
                x['tab_id'] = None
                x['tab_at'] = None
                x['tab_duration'] = None
            else:
                x['tab_id'] = x['tab']['tabId']
                x['tab_at'] = x['tab']['startAt']
                x['tab_duration'] = x['tab']['duration']
            x['tab'] = json.dumps(x['tab'])

            return x
        except Exception as e:
            pprint.pprint(x)
            raise e

    def transform(self, df_raw):
        if df_raw.shape[0] > 0:
            df = df_raw.rename(columns={"trackingId": "tracking_id", "eventId": "event_id"})
            if 'tracking_id' not in df.columns:
                df['tracking_id'] = 'SV-UNDEFINE'
            df['created_at'] = df['_id'].map(lambda x: x.generation_time)
            df['_id'] = df['_id'].map(str)
            df['channel_code'] = df['tracking_id'].fillna('SV-UNDEFINE').map(
                lambda x: x.split('-')[1] if 1 < len(x.split('-')) else None)
            df['ds'] = df['created_at'].map(lambda x: str(x.date()))
            df = df.applymap(lambda x: x.replace('\00', '') if isinstance(x, str) and '\00' in x else x)
            for k in tqdm.tqdm(set(df['event'])):
                _df = df.loc[df['event'] == k]
                pool = mp.Pool(processes=4)
                it = pool.map(self.parse_row, map(lambda x: x[1], _df.iterrows()))
                pool.close()
                pool.join()
                df_norm = pd.DataFrame(it)
                # step 3: load
                self.log.info("# step 3: load")
                self.load(df_norm, k)
        else:
            self.log.info("empty")

    def load(self, df, k):
        drop_cols = [k for k, v in df.isna().all().to_dict().items() if v]
        df = df.drop(columns=drop_cols)
        gcs_path = "user_tracking/ublv2/{}/{}".format(
            k + "_event", self.from_date.astimezone(sg_tz).strftime("%Y-%m-%d/%H%M.pq")
        )
        upload_df_to_gcs(self.gcs_cli, df, self.bucket_name, gcs_path)

    def execute(self):
        self.log.info("# step 1: extract")
        df_raw = self.extract()
        self.log.info("# step 2: transform")
        self.transform(df_raw)
