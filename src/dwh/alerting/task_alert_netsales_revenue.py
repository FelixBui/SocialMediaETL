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

vn_tz = pytz.timezone('Asia/Saigon')
# List date 
ct = datetime.datetime.now(vn_tz)
today = ct.date()
first_date_of_currentmonth = today.replace(day=1)
first_date_of_lastmonth = first_date_of_currentmonth - relativedelta(months=1)
last_date_of_lastmonth  = first_date_of_currentmonth - datetime.timedelta(days=1)
date_of_lastmonth = today - relativedelta(months=1)
yesterday_date = today - datetime.timedelta(days=1)
yesterday_interval1d = today - datetime.timedelta(days=2)
yesterday_interval2d = today - datetime.timedelta(days=3)
current_month = first_date_of_lastmonth.strftime("%Y-%m-%d")
# Metrics
tbls =['dwh_prod.net_sales_alert','dwh_prod.revenue_alert']
mode_test = False
slack_channel = "#test_bot" if mode_test else "#data_alert_bot"


class TaskAlertNetsalesRevenue(AlertingBase):
    def __init__(self, config):
        super().__init__(config)
        self.sheet_id = self.get_param_config(['gsheet_id'])
        self.worksheet_name = self.get_param_config(['worksheet_name'])
        slack_token = self.get_param_conffig(['slack_token'])
        self.slack_client = slack.WebClient(token=slack_token)
        self.bq_creds = self.get_param_config(['dp-ops'])
        self.sa_creds = load_sa_creds(self.bq_creds)
        self.gcreds = load_gsheet_creds(self.bq_creds)

    def extract_accounting_data(self):
        df = load_df_from_gsheet(self.gcreds, self.sheet_id, self.worksheet_name)
        total_ns_monthly = df.loc[ df['month'] == current_month ]['netsales'].sum()
        total_rev_monthly = df.loc[ df['month'] == current_month ]['revenue'].sum()
        return total_ns_monthly, total_rev_monthly
    
    def check_if_weekend(today):
            try:
                isinstance(today, datetime.datetime)
                upper_limit = today + datetime.timedelta(days=(6 - today.weekday()))
                lower_limit = today + datetime.timedelta(days=(5 - today.weekday()))
                if today >= lower_limit <= upper_limit:
                    return "True"
                else:
                    return "False"
            except ValueError:
                print('Your date is not a datetime object.')
    
    def extract(self,from_date,to_date):
        dfs = list()
        for tbl in tbls:
            q = """
            select * except(val,transaction_dt),
                    round(sum(val)) as sum_val
            from `{}`
            where transaction_dt >= '{}' and transaction_dt <= '{}'
            group by 1,2
            order by 1,2
            """.format(tbl,from_date,to_date)
            df = load_df_from_bq(self.sa_creds,q)
            dfs.append(df)
        return dfs

    def standard_metric(self, from_date, to_date, tbl_grp, tbl_name):
        dfs = self.extract(from_date,to_date)
        for df in dfs:
            if len(df) > 0:
                if len(df) > 0 and df.iloc[0]['tbl_grp'] == tbl_grp:
                    standard = df.loc[ df['table_name'] == tbl_name ]['sum_val']
                    standard = float(standard.to_numpy() )
                else:
                    None
        return standard

    def alert(self, dfs, standard_ns, standard_rev, rule_name):
        for df in dfs:
            if df.iloc[0]['tbl_grp'] == 'netsales':
                for ns,tn in zip(df['sum_val'],df['table_name']):
                    i = ns - standard_ns
                    ts = ct.strftime("%Y-%m-%d %H:%M:%S")
                    if i == 0.0:
                        None
                    else:
                        message = """
                        ++ Rule name: {}
                        Message: Inconsistent net sales :no_entry_sign:
                        Level: high
                        Timestamp: {}
                        Description: Table {}'s sum(net_sales_excl_vat) is not equal to sum(net_sales_excl_vat) of odoo_acc_invoice_net_sales_line_items
                        """
                        message = message.format(rule_name, ts, tn)
                        self.slack_client.chat_postMessage(channel= slack_channel, text = message)
                        # print(message)
            elif df.iloc[0]['tbl_grp'] == 'revenue':
                for rev,tn in zip(df['sum_val'],df['table_name']):
                    j = rev - standard_rev
                    ts2 = ct.strftime("%Y-%m-%d %H:%M:%S")
                    if -20000.00 < j < 20000.00:
                        None
                    else:
                        message = """
                        ++ Rule name: {}
                        Message: Inconsistent revenue :no_entry_sign:
                        Level: high
                        Timestamp: {}
                        Description: Table {}'s sum(revenue_excl_vat) is not equal to sum(revenue_excl_vat) of odoo_acc_revenue_transaction
                        """
                        message = message.format(rule_name, ts2, tn)
                        self.slack_client.chat_postMessage(channel= slack_channel, text = message)
                        # print(message)
            else:
                None
   
    def compare_adhoc(self, rule_name):
        sum_rev_current_month = self.standard_metric(first_date_of_currentmonth, today, "revenue","revenue_data_mart")
        sum_rev_last_month =  self.standard_metric(first_date_of_lastmonth, date_of_lastmonth, "revenue","revenue_data_mart")
        sum_rev_yesterday_interval1d = self.standard_metric(yesterday_interval1d, yesterday_interval1d, "revenue","revenue_data_mart")
        print('sum_rev_yesterday_interval1d: {}'.format(sum_rev_yesterday_interval1d))
        sum_rev_yesterday_interval2d  = self.standard_metric(yesterday_interval2d, yesterday_interval2d, "revenue","revenue_data_mart")
        print('sum_rev_yesterday_interval2d: {}'.format(sum_rev_yesterday_interval2d))
        ts = ct.strftime("%Y-%m-%d %H:%M:%S")
        if (sum_rev_yesterday_interval1d < sum_rev_yesterday_interval2d*0.83) or (sum_rev_yesterday_interval1d > sum_rev_yesterday_interval2d*1.17):
            growth = round(((sum_rev_yesterday_interval1d - sum_rev_yesterday_interval2d)/sum_rev_yesterday_interval2d ) * 100 , 2)
            message = """  
            ++ Rule name: {}
            Message: Abnormal revenue change daily :no_entry_sign:
            Level: high
            Timestamp: {}
            Description: Table revenue_data_mart's sum(revenue_excl_vat) on {} growth {} % compare to total revenue on {} 
            """
            message = message.format(rule_name, ts, yesterday_interval1d,growth, yesterday_interval2d)
            self.slack_client.chat_postMessage(channel= slack_channel, text = message)
            # print(message)
        if (sum_rev_current_month < sum_rev_last_month*0.85) or (sum_rev_current_month > sum_rev_last_month*1.05):
            month_name = first_date_of_lastmonth.strftime("%B")
            growth = round(((sum_rev_current_month- sum_rev_last_month)/sum_rev_last_month ) * 100 , 2)
            message = """  
            ++ Rule name: {}
            Message: Abnormal revenue change monthly :no_entry_sign:
            Level: high
            Timestamp: {}
            Description: Table revenue_data_mart's sum(revenue_excl_vat) growth {} % compare to total revenue in {}
            """
            message = message.format(rule_name, ts, growth, month_name)
            self.slack_client.chat_postMessage(channel= slack_channel, text = message)
            # print(message)
        else:
            None

    @AlertingBase.wrapper_simple_log
    def execute(self):
        # raise Exception
        dfs_layer = self.extract(first_date_of_currentmonth,today)
        # print(dfs_layer)
        dfs_acc = self.extract(current_month,last_date_of_lastmonth)
        
        standard_ns = self.standard_metric(first_date_of_currentmonth,today, "netsales","effect_invoice_line_items")
        standard_rev = self.standard_metric(first_date_of_currentmonth,today, "revenue","odoo_acc_revenue_transaction")
        # print("standard_rev: {}".format(standard_rev))
        accounting_m = self.extract_accounting_data()
       
        total_ns_monthly = accounting_m[0]
        total_rev_monthly = accounting_m[1]
        error = 0
        
        try:
            self.alert(dfs_layer, standard_ns, standard_rev, 'Integrity')
            self.slack_client.chat_postMessage(channel= slack_channel, text = '+ Integrity test passed :white_check_mark:')
        except Exception as e:
            print(e)
            error += 1
            self.slack_client.chat_postMessage(channel= slack_channel, text = '+ Integrity test error :no_entry_sign:')
        try:
            self.alert(dfs_acc, total_ns_monthly, total_rev_monthly,'Accounting similarity')
            self.slack_client.chat_postMessage(channel= slack_channel, text = '+ Accounting similarity test passed :white_check_mark:')
        except Exception as e:
            print(e)
            error += 1
            self.slack_client.chat_postMessage(channel= slack_channel, text = '+ Accounting similarity test error :no_entry_sign:')
        try:
            self.compare_adhoc('Comparison threshold based')
            self.slack_client.chat_postMessage(channel= slack_channel, text = '+ Comparison threshold based test passed :white_check_mark:')
        except Exception as e:
            print(e)
            error += 1
            self.slack_client.chat_postMessage(channel= slack_channel, text = '+ Comparison threshold based test error :no_entry_sign:')
        passed = 3 - error
        message = "Finished 3 test cases with passed: {}, error: {} at {} <@U0373UPED4L> :white_check_mark:".format(passed, error, today)
        self.slack_client.chat_postMessage(channel= slack_channel, text = message)
        
        
