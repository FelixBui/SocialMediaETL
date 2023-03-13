from dwh.alerting.alerting_base import AlertingBase
from datetime import timedelta
import datetime
import pandas as pd
import requests
from libs.storage_utils import load_sa_creds, load_gsheet_creds, insert_df_to_gsheet, load_df_from_gsheet, load_df_from_bq
import pytz
import math
from dateutil.relativedelta import relativedelta
import slack
import statistics
from scipy import stats
from scipy.stats import sem
from scipy.stats import t

# timezone
vn_tz = pytz.timezone('Asia/Saigon')
# date range
ct = datetime.datetime.now(vn_tz)
today = ct.date()
yesterday_date = today - datetime.timedelta(days=1)
yesterday_interval1d = today - datetime.timedelta(days=2)
last_14days_date = yesterday_interval1d - datetime.timedelta(days=14)
# date_string = '2022-01-01'
# first_date_of_2022 = datetime.datetime.strptime(date_string, '%Y-%m-%d')
# first_date_of_2022 = first_date_of_2022.date()


# metrics
tbls =['dwh_prod.net_sales_alert','dwh_prod.revenue_alert']
tbl_name = ['dwh_fact_sales_order_items','dwh_fact_revenue']
mode_test = False
slack_channel = "#test_bot" if mode_test else "#data_alert_bot"


class TaskAnomalyDetection(AlertingBase):
    def __init__(self, config):
        super().__init__(config)
        slack_token = self.get_param_config(['slack_token'])
        self.slack_client = slack.WebClient(token=slack_token)
        self.bq_creds = self.get_param_config(['dp-ops'])
        self.sa_creds = load_sa_creds(self.bq_creds)
        self.gcreds = load_gsheet_creds(self.bq_creds)

    def extract(self, from_date,to_date):
        dfs = list()
        for tbl, name in zip(tbls, tbl_name):
            q = """
            select  * except(val, tbl_grp, table_name),
                    round(sum(val)) as sum_val
            from `{}`
            where transaction_dt >= '{}' and transaction_dt < '{}'
            and table_name = '{}'
            group by 1
            """.format(tbl,from_date, to_date, name)
            df = load_df_from_bq(self.sa_creds,q)
            dfs.append(df)
        return dfs

    def rules(self, from_date,to_date):
        dfs = self.extract(from_date,to_date)
        for df in dfs:
            if len(dfs[0]) < len(dfs[1]) and len(df) == len(dfs[0]):
                df_ns = df
                mean_ns = statistics.mean(df['sum_val'])
                stdv_ns = statistics.stdev(df['sum_val'])
            else:
                df_rev = df
                mean_rev = statistics.mean(df['sum_val'])
                stdv_rev = statistics.stdev(df['sum_val'])
        return df_ns, mean_ns, stdv_ns, df_rev, mean_rev, stdv_rev

    def anomaly_detection(self, from_date, to_date):
        r = self.rules(from_date,to_date)
        df_ns = r[0]
        mean_ns = r[1]
        stdv_ns = r[2]
        df_rev = r[3]
        mean_rev = r[4]
        stdv_rev = r[5]
        for i, j in zip(df_ns['sum_val'], df_ns['transaction_dt']):
            a1 = df_ns.loc[ df_ns['transaction_dt'] == yesterday_date ]['sum_val'].sum()
            a2 = df_ns.loc[ df_ns['transaction_dt'] == yesterday_interval1d ]['sum_val'].sum()
            if (mean_ns - 2*stdv_ns) < i < (mean_ns + 2*stdv_ns):
                None
            elif j == yesterday_date:
                growth = round(((a1 - a2)/a2 ) * 100 , 2)
                message = """
                    ++ Rule name: Anomaly detection
                        Message: Abnormal net sales change daily :no_entry_sign:
                        Level: high
                        Timestamp: {}
                        Description: Table sales_order_items_data_mart's sum(net_sales_excl_vat) growth {} % compare to yesterday's net sales
                """
                message = message.format(ct, growth)
                # print(message)
                self.slack_client.chat_postMessage(channel= slack_channel, text = message)
        for x, y in zip(df_rev['sum_val'], df_rev['transaction_dt']):
            a1 = df_rev.loc[ df_rev['transaction_dt'] == yesterday_date ]['sum_val'].sum()
            a2 = df_rev.loc[ df_rev['transaction_dt'] == yesterday_interval1d ]['sum_val'].sum()
            if (mean_rev - 2*stdv_rev) < x < (mean_rev + 2*stdv_rev):
                None
            elif y == yesterday_date:
                growth = round(((a1 - a2)/a2 ) * 100 , 2)
                message = """
                    ++ Rule name: Anomaly detection
                        Message: Abnormal revenue change daily :no_entry_sign:
                        Level: high
                        Timestamp: {}
                        Description: Table revenue_data_mart's sum(net_sales_excl_vat) growth {} % compare to yesterday's revenue
                """
                message = message.format(ct, growth)
                # print(message)
                self.slack_client.chat_postMessage(channel= slack_channel, text = message)

    def execute(self):
        error = 0
        try:
            self.anomaly_detection(last_14days_date, yesterday_interval1d)
            self.slack_client.chat_postMessage(channel= slack_channel, text = '+ Anomaly detection passed :white_check_mark:')
        except Exception as e:
            print(e)
            error += 1
            self.slack_client.chat_postMessage(channel= slack_channel, text = '+ Anomaly detection error :no_entry_sign:')
        passed = 1 - error
        message = "Finished 1 test cases with passed: {}, error: {} at {} <@U0373UPED4L> :white_check_mark:".format(passed, error, today)
        self.slack_client.chat_postMessage(channel= slack_channel, text = message)
        # print(message)
