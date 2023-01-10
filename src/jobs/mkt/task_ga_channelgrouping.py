from dateutil import tz
from core.app_base import AppBase

import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import timedelta
from time import sleep
from random import random
from apiclient.errors import HttpError
from libs.storage_utils import load_sa_creds
import pytz
vn_tz = pytz.timezone('Asia/Saigon')

local_tz = tz.tzlocal()
utc_tz = tz.tzutc()

class TaskGaChannelgrouping(AppBase):
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
        self.channel_info = self.get_param_config(['channel_info'])
        self.full_metrics = None
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

    @staticmethod
    def get_service(api_name, api_version, credentials):
        service = build(api_name, api_version, credentials=credentials)
        return service

    def get_goal_list_by_channel(self, channel_name):
        service = self.get_service(
                api_name='analytics',
                api_version='v3',
                credentials= self.ga_creds)

        res = service.management().goals().list(
            accountId= self.channel_info[channel_name]['account_id'],
            webPropertyId= self.channel_info[channel_name]['web_prop_id'],
            profileId= self.channel_info[channel_name]['profile_id']
        ).execute()
        items = res['items']
        goal_lst = list()
        for item in items:
            goal_lst .append('ga:goal{}Completions'.format(item['id']))
        return goal_lst

    def get_goal_list_all_channel(self):
        vl24h = self.get_goal_list_by_channel("Vieclam24h")
        sleep(2)
        tvn = self.get_goal_list_by_channel("Timviecnhanh")
        sleep(2)
        vtn = self.get_goal_list_by_channel("Viectotnhat")
        sleep(2)
        mw = self.get_goal_list_by_channel("Mywork")
        sleep(2)
        full = vl24h + tvn + vtn + mw
        final_goal_lst = list()
        for goal in full:
            if goal not in final_goal_lst:
                final_goal_lst.append(goal)
        return final_goal_lst

    def get_full_goal_metrics_parameter(self):
        para_lst = list()
        goal_lst = self.get_goal_list_all_channel()
        for goal in goal_lst:
            para_dict = dict()
            para_dict['expression'] = goal
            para_lst.append(para_dict)
        return para_lst

    def init_full_metrics(self):
        fix_metrics = self.report_parameter['metrics']
        goal_metrics = self.get_full_goal_metrics_parameter()
        self.full_metrics = fix_metrics + goal_metrics

    def get_channelGrouping_metrics_by_channel(self,channel_name, start_date, end_date):
        # init report
        analytics = self.initialize_analyticsreporting()
        # get view_id from channel
        view_id = self.mapping_channel_viewid[channel_name]
        # read dimensions and metrics
        dimensions = self.report_parameter['dimensions']
        metrics = self.report_parameter['metrics']

        # base parameter viewId-daterange
        para_dict = dict()
        para_dict['viewId'] = str(view_id)
        para_dict['dateRanges'] = dict()
        para_dict['dateRanges']['startDate'] = start_date
        para_dict['dateRanges']['endDate'] = end_date
        para_dict['pageSize'] = 100000


        # base table -> with full source medium campaign base on sesssion for left join all small metrics table
        # source medium campaign channel_grouping - sessions
        dimensions_lst = list()
        for dimension in dimensions:
            dimensions_lst.append(dimension)
        para_dict['dimensions']  = dimensions_lst
        session_dict = dict()
        session_dict['expression'] = 'ga:sessions'
        para_dict['metrics'] = [session_dict]
        response = self.get_report(analytics,para_dict)
        df_base = self.response_to_dataframe(response)
        sleep(1)

        # list to store all metrics
        df_lst = list()
        # split to small group since limitation
        extract_metric = self.full_metrics[1:]
        chunk_lst =[extract_metric[i:i + self.max_total_metric] for i in range(0, len(extract_metric), self.max_total_metric)]
        for el in chunk_lst:
            para_dict['metrics'] = list()
            for metric in el:
                para_dict['metrics'].append(metric)
            
            # get data with para_dict
            response = self.get_report(analytics,para_dict)
            df = self.response_to_dataframe(response)
            if df.empty != True:
                df_lst.append(df)
            sleep(1)

        # consolidate all metric
        for df in df_lst:
            df_base = pd.merge(df_base, df, how='left', on=['ga:channelGrouping','ga:source','ga:medium','ga:campaign'])
        df_full = df_base.copy()
        df_full['channel_name'] = channel_name
        return df_full

    def extract_by_channel_per_day(self, channel_name, date):
        df = self.get_channelGrouping_metrics_by_channel(channel_name, date, date)
        df['start_date'] = date
        df['start_date'] = pd.to_datetime(df['start_date']).dt.date
        return df

    def extract(self):
        date = self.from_date
        if not self.is_backfill:
            date = self.from_date - timedelta(days= self.days_to_subtract)
        date_str = date.strftime("%Y-%m-%d")
        tvn = self.extract_by_channel_per_day("Timviecnhanh", date_str)
        tvn = self.transform_to_ga_metrics_schema(tvn)
        sleep(2)
        vl24h = self.extract_by_channel_per_day("Vieclam24h", date_str)
        vl24h = self.transform_to_ga_metrics_schema(vl24h)
        sleep(2)
        mw = self.extract_by_channel_per_day("Mywork", date_str)
        mw = self.transform_to_ga_metrics_schema(mw)
        sleep(2)
        vtn = self.extract_by_channel_per_day("Viectotnhat", date_str)
        vtn = self.transform_to_ga_metrics_schema(vtn)
        rs = pd.concat([tvn, vl24h, mw, vtn])
        return rs

    def transform(self, df):
        return df
    
    # transform to ga_metrics table schema
    def transform_to_ga_metrics_schema(self, df):
        metrics = self.full_metrics
        metric_name_lst = list()
        for el in metrics:
            metric_name_lst.append(el['expression'])
        
        rs_lst = list()
        for index, row in df.iterrows():
            # ignore dimension column
            col_lst = df.columns
            for col in col_lst:
                if col in metric_name_lst:
                    if float(row[col]) > 0.0 :
                        rc = dict()
                        rc['channelGrouping'] = row['ga:channelGrouping']
                        rc['source'] = row['ga:source']
                        rc['medium'] = row['ga:medium']
                        rc['campaign'] = row['ga:campaign']
                        rc['metric_name'] = col
                        rc['metric_value'] = row[col]
                        rc['channel_name'] = row['channel_name']
                        rc['date_start'] = row['start_date']
                        rs_lst.append(rc)

        rs_df = pd.DataFrame(rs_lst)
        rs_df = rs_df[['source','medium','campaign','channelGrouping','metric_name','metric_value','channel_name','date_start']]
        rs_df['metric_value'] = rs_df['metric_value'].astype('float64')
        return rs_df

    def load(self, df):
        df.to_gbq(
            self.destination, 
            if_exists= self.insert_mode, 
            table_schema= self.schema, 
            credentials= self.sa_creds)

    def execute(self):
        if self.full_metrics is None:
            self.init_full_metrics()
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
        
        


