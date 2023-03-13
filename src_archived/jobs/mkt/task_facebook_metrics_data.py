import datetime
from datetime import timedelta
import pandas as pd
from core.app_base import AppBase
import requests
from libs.storage_utils import load_sa_creds
from libs.utils import make_datetime_string_date
import pytz

vn_tz = pytz.timezone('Asia/Saigon')

class TaskFacebookMetricsData(AppBase):

    def __init__(self, config):
        super(TaskFacebookMetricsData, self).__init__(config)
        self.fb_access_token = self.get_param_config(['mkt-auth', 'facebook'])
        self.dp_ops = self.get_param_config(['dp-ops'])
        self.sa_creds = load_sa_creds(self.dp_ops)
        self.params_config = self.get_param_config(['api_params'])
        self.level = self.get_param_config(['level'])
        self.channels = self.get_param_config(['channels'])
        self.from_date = self.get_process_info(["from_date"]).astimezone(vn_tz)
        self.to_date = self.get_process_info(["to_date"]).astimezone(vn_tz)
        self.is_backfill = False
        self.days_to_subtract = self.get_param_config(['days_to_subtract'])

    @staticmethod
    def request_insights_data(access_token, ad_account, fields, level, date_):
        request_type = "insights"

        params = {
            "fields": fields,
            "level": level,
            "access_token": access_token,
            "filtering": """
            [{
                "field":"spend",
                "operator":"GREATER_THAN",
                "value":0
            }]
                """,
            "time_range": "{{'since':'{}','until':'{}'}}".format(date_, date_),
            "limit": 500
        }
        base_url = "https://graph.facebook.com/v13.0/{}/{}".format(ad_account, request_type)
        response = requests.get(base_url, params=params)
        content = response.json()

        return content['data']

    def extract(self):
        current_d_str = make_datetime_string_date(self.from_date)
        if not self.backfill:
            date = self.from_date - timedelta(days= self.days_to_subtract)
            current_d_str = make_datetime_string_date(date)
        access_token = self.fb_access_token
        level = self.level
        channels = self.channels
        fields = self.params_config[level]['metric_fields']

        content_data = []
        for channel in channels:
            ad_account = channel['id']
            channel_insights = self.request_insights_data(access_token, ad_account, fields, level, current_d_str)
            content_data.append(channel_insights)
        return content_data

    def transform(self, content_data):
        level = self.level
        dfcols = self.params_config[level]['dfcols']
        dtypes = self.params_config[level]['dtypes']

        df_list = []
        for i, channel_data in enumerate(content_data):
            df = pd.DataFrame(channel_data, columns=dfcols)
            df_list.append(df)

        df_res = pd.concat(df_list)
        df_res['currency_unit'] = pd.Series(['USD' for _ in range(df_res.shape[0])])
        df_res.reset_index(inplace=True, drop=True)
        for t in dtypes:
            df_res[t] = df_res[t].astype(dtypes[t])

        return df_res

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

    def backfill(self, interval=1):
        self.is_backfill = True
        from_date = self.from_date
        to_date = self.to_date
        seed_date = from_date
        while seed_date <= to_date:
            self.execute()
            seed_date += datetime.timedelta(days=interval)
            self.from_date = seed_date