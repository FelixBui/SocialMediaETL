import datetime
from dateutil import tz
import pandas as pd
from core.app_base import AppBase
import requests
from libs.storage_utils import load_sa_creds
from libs.utils import extract_utm_field_from_url
from time import sleep
import pytz

vn_tz = pytz.timezone('Asia/Saigon')

class TaskFacebookAdDim(AppBase):
    def __init__(self, config):
        super(TaskFacebookAdDim, self).__init__(config)
        self.fb_access_token = self.get_param_config(['mkt-auth', 'facebook'])
        self.dp_ops = self.get_param_config(['dp-ops'])
        self.sa_creds = load_sa_creds(self.dp_ops)
        self.params_config = self.get_param_config(['api_params'])
        self.level = self.get_param_config(['level'])
        self.channels = self.get_param_config(['channels'])
        self.from_date = self.get_process_info(["from_date"])
        self.to_date = self.get_process_info(["to_date"])

    @staticmethod
    def request_dim_data(access_token, ad_account, fields, level):
        request_type = "ads"
        params = {
            "fields": fields,
            "level": level,
            "access_token": access_token,
            "limit": 500
        }
        base_url = "https://graph.facebook.com/v13.0/{}/{}".format(ad_account, request_type)
        response = requests.get(base_url, params=params)
        content = response.json()

        dim_data = []
        has_next = True
        ###### ! TEST CODE #####
        # ite = 1
        # lim_ite = 1
        ##################### !
        while has_next:
            try:
                dim_data += content['data']
                if 'next' in content['paging']:
                    response = requests.get(content['paging']['next'])
                    content = response.json()
                else:
                    has_next = False
                ######! TEST code ####
                # if ite < lim_ite:
                #     ite += 1
                # else:
                #     has_next = False
                ############## !
            except Exception as e:
                print(e)
                print(content)
        return dim_data

    @staticmethod
    def parse_dim_info(record):
        channel_dict = {
                        '1304144062976180': 'Vieclam24h',
                        '1190261891031065': 'Timviecnhanh',
                        '1540603222663595': 'Viectotnhat',
                        '1188120964693133': 'Mywork'
                        }
        ad_id = record['id']
        adgroup_id = record['adset']['id']
        try:
            url_tags = record['adcreatives']['data'][0]['url_tags']
        except KeyError as e:
            url_tags = ""
        name = record['name']
        channel_name = channel_dict[record['account_id']]
        utm_source = extract_utm_field_from_url(url_tags, 'source')
        utm_medium = extract_utm_field_from_url(url_tags, 'medium')
        utm_campaign = extract_utm_field_from_url(url_tags, 'campaign')
        mkt_channel = "Facebook"

        return adgroup_id,ad_id,name,utm_source,utm_medium,utm_campaign,url_tags,channel_name,mkt_channel

    def extract(self):
        access_token = self.fb_access_token
        level = self.level
        channels = self.channels
        fields = self.params_config[level]['query_fields']

        content_data = []
        for channel in channels:
            ad_account = channel['id']
            channel_dim_data = self.request_dim_data(access_token, ad_account, fields, level)
            content_data += channel_dim_data

        return content_data

    def transform(self, content_data):
        level = self.level
        dfcols = self.params_config[level]['dfcols']
        dtypes = self.params_config[level]['dtypes']

        records = []
        for i, rec in enumerate(content_data):
            parsed_rec = self.parse_dim_info(rec)
            records.append(parsed_rec)
        df = pd.DataFrame(records, columns=dfcols)

        for t in dtypes:
            df[t] = df[t].astype(dtypes[t])

        return df

    def load(self, df):
        current_level = self.level
        destination = self.params_config[current_level]['destination']
        insert_mode = self.params_config[current_level]['insert_mode']
        schema = self.params_config[current_level]['schema']

        df.to_gbq(destination, if_exists=insert_mode, table_schema=schema, credentials=self.sa_creds)

    def execute(self):
        content = self.extract()
        df_res = self.transform(content)
        self.load(df_res)
        sleep(120)

    def backfill(self, interval=1):
        from_date = self.from_date
        to_date = self.to_date
        seed_date = from_date
        while seed_date <= to_date:
            self.execute()
            seed_date += datetime.timedelta(days=interval)
            self.from_date = seed_date


        