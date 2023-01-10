from dateutil import tz
import json
from urllib.parse import urlsplit, parse_qs
import pandas as pd
from dwh.user_tracking.user_tracking_base import UserTrackingBase
from libs.storage_utils import load_sa_creds, get_bq_cli, get_gcs_cli, load_df_from_gcs_file, load_df_from_gcs_folder
from user_agents import parse as parse_ua
import gcsfs
import pytz
import re
from datetime import datetime, timedelta
from tqdm import tqdm

local_tz = tz.tzlocal()
sg_tz = pytz.timezone("Asia/Saigon")
utc_tz = tz.tzutc()


class TaskEtlCustomUblv2(UserTrackingBase):
    def __init__(self, config):
        super(TaskEtlCustomUblv2, self).__init__(config)
        self.from_date = self.from_date.astimezone(sg_tz).replace(minute=0, second=0, microsecond=0)
        self.to_date = self.to_date.astimezone(sg_tz).replace(minute=0, second=0, microsecond=0)
        self.dp_ops = self.get_param_config(['dp-ops'])
        self.gcs_cli = get_gcs_cli(self.dp_ops)

        self.bq_creds = load_sa_creds(self.dp_ops)
        self.events = self.get_param_config(['events'])
        self.bucket_name = self.get_param_config(['bucket_name'])
        self.project_id = self.get_param_config(['project_id'])
        self.gcs_path = self.get_param_config(['gcs_path'])

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
    def parse(cls, x):
        device_ua = cls.parse_user_agent(x['device_ua']) if not pd.isna(x['device_ua']) else x['device_ua']
        source = cls.parse_url(x['url'])
        x['device_info'] = device_ua
        x['source_info'] = source
        return x

    @staticmethod
    def generate_date(start, end):
        delta = end - start  # as timedelta
        days = [start + timedelta(days=i) for i in range(delta.days + 1)]
        return days

    def is_event_match(self, x):
        for event in self.events:
            if re.search(r'^{}_'.format(event), x):
                return True
        return False
        
    def extract(self):
        fs = gcsfs.GCSFileSystem(project=self.project_id, token=self.dp_ops)
        file_names = fs.ls('{}/{}'.format(self.bucket_name, self.gcs_path))
        file_names = [el.split('/')[-1] for el in file_names]
        
        event_folder_path = list()
        for el in file_names:
            if self.is_event_match(el):
                event_folder_path.append(el)

        get_lst = list()
        lst_dfs = list()
        sg_dt = self.from_date.replace(tzinfo=sg_tz).date()
        time_str = str(self.from_date.replace(tzinfo=sg_tz).time()).split(':')
        time_str = time_str[0] + time_str[1] 
        for event_path in event_folder_path:
            date_folder_path = fs.ls('{}/{}/{}'.format(
                self.bucket_name, self.gcs_path, event_path))
            for date_path in date_folder_path:
                if str(sg_dt) in date_path:
                    time_file_path = fs.ls(date_path)
                    for time_path in time_file_path:
                        if time_str in time_path:
                            get_lst.append(event_path)
                            
        for path in get_lst:
            df = load_df_from_gcs_file(
                self.dp_ops, 
                self.project_id, 
                self.bucket_name, 
                self.gcs_path + '/' + path + '/' + str(sg_dt) + '/' + time_str)
            lst_dfs.append(df)
                    
        if len(lst_dfs):
            df_raw = pd.concat(lst_dfs, ignore_index=True)
            return df_raw
        else:
            exit(0)

    def extract_range(self):
        start_date = self.from_date
        end_date = self.to_date
        date_lst = self.generate_date(start_date, end_date)
        date_lst = [el.replace(tzinfo=sg_tz).date() for el in date_lst]
        str_date_lst = [ str(el) for el in date_lst]

        fs = gcsfs.GCSFileSystem(project="data-warehouse-sv", token="dp-dev.json")
        file_names = fs.ls('{}/{}'.format("dp-raw-data", "user_tracking/ublv2"))
        file_names = [el.split('/')[-1] for el in file_names]

        # just get event we need
        event_folder_path = list()
        for el in file_names:
            if self.is_event_match(el):
                event_folder_path.append(el)

        get_lst = list()
        lst_dfs = list()

        # for each event -> get all data we need
        for event_path in event_folder_path:
            # for each event get date folder we need
            date_folder_path = fs.ls('{}/{}/{}'.format(
                "dp-raw-data", "user_tracking/ublv2", event_path))
            for date_path in date_folder_path:
                date_path_date = str(date_path).split('/')[4]
                if date_path_date in str_date_lst:
                    valid_path = date_path.split('/')[-2] + '/' + date_path.split('/')[-1]
                    get_lst.append(valid_path)

        for index in tqdm(range(len(get_lst))):
            df = load_df_from_gcs_folder(
                self.dp_ops,
                self.project_id,
                self.bucket_name,
                self.gcs_path + '/' + get_lst[index])
            lst_dfs.append(df)

        if len(lst_dfs):
            df_raw = pd.concat(lst_dfs, ignore_index=True)
            return df_raw
        else:
            exit(0)

    def extract_daily(self):
        pass

    def transform(self, df_raw):
        df = df_raw.apply(self.parse, axis=1)
        df = df.applymap(lambda x: x.replace("\x00", '') if isinstance(x, str) and "\x00" in x else x)

        df['source_info'] = df['source_info'].map(lambda x: json.dumps(x) if isinstance(x, dict) else x)
        df['device_info'] = df['device_info'].map(lambda x: json.dumps(x) if isinstance(x, dict) else x)
        df['context_data'] = df['context_data'].map(lambda x: json.dumps(x) if isinstance(x, dict) else x)
        if 'globalDeviceId' not in df.columns:
            df['globalDeviceId'] = None

        return df

    def load(self, df):
        bq_cli = get_bq_cli(self.dp_ops)
        for event in self.events:
            # print(df.iloc[0])
            if event == 'pnt':
                df_sub = df[df['target'].map(lambda x: json.loads(x).get('type', '') == 'personality_test')]
            elif event == 'click':
                df_sub = df[df['event'].str.startswith(event)]
                event = 'click_event'
            elif event == 'page_view':
                df_sub = df[df['event'].str.startswith(event)]
                event = 'page_view_event'
            elif event == 'select':
                df_sub = df[df['event'].str.startswith(event)]
                event = 'select_event'
            else:
                df_sub = df[df['event'].str.startswith(event)]
            if df_sub.shape[0] > 0:
                print(event)
                print(df_sub.iloc[0])
                q = """delete from ublv2.{} where created_at >='{}' and created_at <= '{}' """.format(
                    event, df_sub['created_at'].min(), df_sub['created_at'].max()
                )
                j = bq_cli.query(q)
                j.result()
                bq_cli = get_bq_cli(self.dp_ops)
                dataset_ref = bq_cli.dataset("ublv2", project='data-warehouse-sv')
                table_ref = dataset_ref.table(event)
                table = bq_cli.get_table(table_ref)  # API Request
                schema = table.schema
                _schema = []
                for field in schema:
                    _schema.append({'name': field.name, 'type': field.field_type})
                df_sub = df_sub[[
                    '_id', 'platform', 'ts', 'tz', 'context', 'attributes', 'target',
                    'device', 'session', 'tab', 'event', 'event_id', '__v', 'tracking_id',
                    'created_at', 'channel_code', 'ds', 'context_id', 'context_type',
                    'context_data', 'url', 'referrer', 'device_id', 'device_at',
                    'device_ua', 'device_sw', 'device_sh', 'device_sd', 'device_tz',
                    'device_info', 'source_info', 'session_id', 'session_at', 'globalDeviceId',
                    'session_duration', 'tab_id', 'tab_at', 'tab_duration', 'exp_device_id',
                    'exp_session_id']]
                df_sub = df_sub.drop_duplicates(['_id'])
                df_sub.to_gbq(
                    "ublv2.{}".format(event),
                    if_exists="append", credentials=self.bq_creds, table_schema=_schema)

    def execute(self):
        print("extract")
        # df_raw = self.extract_range()
        df_raw = self.extract()
        print("transform")
        df = self.transform(df_raw)
        print("load")
        self.load(df)
