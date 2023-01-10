from core.app_base import AppBase
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import timedelta
from time import sleep
from random import random
from apiclient.errors import HttpError
from random import random
from libs.storage_utils import load_sa_creds
import pytz

vn_tz = pytz.timezone('Asia/Saigon')

class TaskGaUsersMetrics(AppBase):
    def __init__(self, config):
        super().__init__(config)
        self.dp_ops = self.get_param_config(['dp-ops'])
        self.sa_creds = load_sa_creds(self.dp_ops)
        self.scopes_readonly = self.get_param_config(['scopes_readonly'])
        self.ga_creds = service_account.Credentials.from_service_account_info(self.get_param_config(['dp-ops']))
        self.from_date = self.get_process_info(['from_date']).astimezone(vn_tz)
        self.to_date = self.get_process_info(['to_date']).astimezone(vn_tz)
        self.max_total_metric = self.get_param_config(['max_total_metric'])
        self.max_total_dimension = self.get_param_config(['max_total_dimension'])
        self.mapping_channel_viewid = self.get_param_config(['mapping_channel_viewid'])
        self.report_parameter = self.get_param_config(['report_parameter']) 
        self.destination = self.get_param_config(['destination'])
        self.schema = self.get_param_config(['schema'])
        self.insert_mode = self.get_param_config(['insert_mode'])
        self.days_to_subtract = self.get_param_config(['days_to_subtract'])
        self.is_backfill = False

    def initialize_analyticsreporting(self):
        analytics = build('analyticsreporting', 'v4', credentials= self.ga_creds)
        return analytics

    @staticmethod
    def get_report(analytics, report_parameter ):
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    report_parameter
                ]
            }  
        ).execute()
    
    @staticmethod
    def response_to_dataframe(response):
        row_list = []
        for report in response.get('reports', []):
            column_header = report.get('columnHeader', {})
            dimension_headers = column_header.get('dimensions', [])
            metric_headers = column_header.get('metricHeader', {}).get('metricHeaderEntries', [])
        
            for row in report.get('data', {}).get('rows', []):
                row_dict = {}
                dimensions = row.get('dimensions', [])
                date_range_values = row.get('metrics', [])
                
                for header, dimension in zip(dimension_headers, dimensions):
                    row_dict[header] = dimension

                # Fill dict with metric header (key) and metric value (value)
                for i, values in enumerate(date_range_values):
                    for metric, value in zip(metric_headers, values.get('values')):
                    # Set int as int, float a float
                        if ',' in value or '.' in value:
                            row_dict[metric.get('name')] = float(value)
                        else:
                            row_dict[metric.get('name')] = int(value)

                row_list.append(row_dict)
        return pd.DataFrame(row_list)

    def get_users_metrics_by_channel(self,channel_name, start_date, end_date):
        # init report
        analytics = self.initialize_analyticsreporting()
        # get view_id from channel
        view_id = self.mapping_channel_viewid[channel_name]
        # read dimensions and metrics
        metrics = self.report_parameter['metrics']

        para_dict = dict()
        para_dict['viewId'] = str(view_id)
        para_dict['dateRanges'] = dict()
        para_dict['dateRanges']['startDate'] = start_date
        para_dict['dateRanges']['endDate'] = end_date
        para_dict['pageSize'] = 100000
        para_dict['metrics'] = metrics
        response = self.get_report(analytics,para_dict)
        df = self.response_to_dataframe(response)
        df['channel_name'] = channel_name
        return df

    def extract_by_channel_per_day(self, channel_name, date):
        df = self.get_users_metrics_by_channel(channel_name, date, date)
        df['date_start'] = date
        df['date_start'] = pd.to_datetime(df['date_start']).dt.date
        df = df.rename(columns={'ga:users': 'users'}, errors='raise')
        df = df[['users','date_start','channel_name']]
        return df

    def extract(self):
        date = self.from_date
        if not self.is_backfill:
            date = self.from_date - timedelta(days= self.days_to_subtract)
        date_str = date.strftime("%Y-%m-%d")
        tvn = self.extract_by_channel_per_day("Timviecnhanh", date_str)
        sleep(3)
        vl24h = self.extract_by_channel_per_day("Vieclam24h", date_str)
        sleep(3)
        mw = self.extract_by_channel_per_day("Mywork", date_str)
        sleep(3)
        vtn = self.extract_by_channel_per_day("Viectotnhat", date_str)
        rs = pd.concat([tvn, vl24h, mw, vtn])
        return rs

    def transform(self, df):
        return df
    
    def load(self, df):
        df.to_gbq(
            self.destination, 
            if_exists= self.insert_mode, 
            table_schema= self.schema, 
            credentials= self.sa_creds)

    def execute(self):
        df = self.extract()
        df = self.transform(df)
        self.load(df)
        sleep(300)
           
    def backfill(self, interval=1):
        self.is_backfill = True
        from_date = self.from_date
        to_date = self.to_date
        seed_date = from_date
        while seed_date <= to_date:
            for t in range(0,5):
                try:
                    print(seed_date)
                    df = self.execute()
                    seed_date += timedelta(days=interval)
                    self.from_date = seed_date
                    sleep(5)
                    break
                except HttpError as error:
                    if error.resp.reason in ['userRateLimitExceeded', 'quotaExceeded','internalServerError', 'backendError']:
                        print("out of limit")
                        sleep(60)
                    elif 'The service is currently unavailable' in str(error):
                        print("The service is currently unavailable")
                        sleep((2 ** t) + random())   



