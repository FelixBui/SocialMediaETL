from dwh.alerting.alerting_base import AlertingBase
import unittest
from datetime import timedelta
import datetime
import pandas as pd
import requests
from libs.storage_utils import load_sa_creds, load_df_from_bq, load_gsheet_creds
import pytz
import math
from dateutil.relativedelta import relativedelta
import slack

vn_tz = pytz.timezone('Asia/Saigon')
#list date
ct = datetime.datetime.now(vn_tz)
today = ct.date()
mode_test = False
slack_channel = "#test_bot" if mode_test else "#data_alert_bot"
#metrics
dds_list = [('company_hash_id', 'dwh_prod_dds.dwh_dim_sv_company_hash'),
    ('salary_range_id', 'dwh_prod_dds.dwh_dim_salary_range'),
    ('sv_cus_code', 'dwh_prod_dds.dwh_dim_sv_customer'),
    ('sales_order_id_key', 'dwh_prod_dds.dwh_dim_sales_order'),
    ('sku_code', 'dwh_prod_dds.dwh_dim_sku'),
    ('company_id_key', 'dwh_prod_dds.dwh_dim_competitor_company'),
    ('channel_code', 'dwh_prod_dds.dwh_dim_channel'),
    ('tax_code', 'dwh_prod_dds.dwh_dim_company_size'),
    # ('tax_code', 'dwh_prod_dds.dwh_dim_market_company_overlap'),
    ('job_id_key', 'dwh_prod_dds.dwh_dim_competitor_job_post'),
    ('job_id_key', 'dwh_prod_dds.dwh_dim_job'),
    ('seeker_id_key', 'dwh_prod_dds.dwh_dim_seeker'),
    ('emp_id_key', 'dwh_prod_dds.dwh_dim_employer'),
    ('emp_id_key', 'dwh_prod_dds.dwh_dim_employer_freemium'),
    ('tax_code', 'dwh_prod_dds.dwh_dim_company_region'),
    ('resume_id_key', 'dwh_prod_dds.dwh_dim_resume'),
    ('prov_id', 'dwh_prod_dds.dwh_dim_province' ) ]

mart_list = [('emp_id_key', 'dwh_prod_mart.employer_freemium_data_mart'),
    ('emp_source', 'dwh_prod_mart.employer_freemium_data_mart'),
    ('is_employer_freemium', 'dwh_prod_mart.employer_freemium_data_mart'),
    # ('sv_cus_code', 'dwh_prod_mart.employer_freemium_data_mart'),
    ('sales_order_items_sub_id_key', 'dwh_prod_mart.employer_purchase_timeline_data_mart'),
    ('sales_order_id_key', 'dwh_prod_mart.employer_purchase_timeline_data_mart'),
    ('emp_id_key', 'dwh_prod_mart.employer_purchase_timeline_data_mart'),
    ('service_type', 'dwh_prod_mart.employer_usage_timeline_data_mart'),
    # ('prod_id_key', 'dwh_prod_mart.employer_usage_timeline_data_mart'),
    ('unit', 'dwh_prod_mart.employer_usage_timeline_data_mart'),
    ('qty_used', 'dwh_prod_mart.employer_usage_timeline_data_mart'),
    ('emp_id_key', 'dwh_prod_mart.employer_usage_timeline_data_mart'),
    # ('seeker_id_key', 'dwh_prod_mart.employer_view_resume_data_mart'),
    ('resume_viewed_id_key', 'dwh_prod_mart.employer_view_resume_data_mart'),
    # ('resume_id_key', 'dwh_prod_mart.employer_view_resume_data_mart'),
    ('emp_id_key', 'dwh_prod_mart.employer_view_resume_data_mart'),
    ('job_id_key', 'dwh_prod_mart.job_post_data_mart'),
    ('seeker_id_key', 'dwh_prod_mart.job_seeker_data_mart'),
    ('job_id_key', 'dwh_prod_mart.market_job_post_data_mart'),
    ('channel_name', 'dwh_prod_mart.market_job_post_data_mart'),
    ('channel_owner', 'dwh_prod_mart.market_job_post_data_mart'),
    ('start_dt', 'dwh_prod_mart.market_job_post_data_mart'),
    ('end_dt', 'dwh_prod_mart.market_job_post_data_mart'),
    ('resume_id_key', 'dwh_prod_mart.resume_data_mart'),
    ('emp_id_key', 'dwh_prod_mart.revenue_data_mart'),
    ('sku_code', 'dwh_prod_mart.revenue_data_mart'),
    ('dt', 'dwh_prod_mart.revenue_data_mart'),
    ('sales_order_id_key', 'dwh_prod_mart.sales_order_data_mart'),
    ('sales_order_items_id_key', 'dwh_prod_mart.sales_order_items_data_mart'),
    # ('job_id_key', 'dwh_prod_mart.seeker_apply_job_data_mart') 
    ]

message_fmt = """```
Rule type: {}
Time at: {}
Successed: {}
Failed: {}
Detail: {}
```"""

class UnitTest(AlertingBase):
    def __init__(self, config):
        super().__init__(config)
        self.sheet_id = self.get_param_config(['gsheet_id'])
        self.worksheet_name = self.get_param_config(['worksheet_name'])
        slack_token = self.get_param_config(['slack_token'])
        self.slack_client = slack.WebClient(token=slack_token)
        self.bq_creds = self.get_param_config(['dp-ops'])
        self.sa_creds = load_sa_creds(self.bq_creds)
        self.gcreds = load_gsheet_creds(self.bq_creds)
        
    def extract(self):
        df = load_df_from_gsheet(self.gcreds, self.sheet_id, self.worksheet_name)
        return df

    def transform(self,table_list):
        dfs = list()
        for col,tbl in table_list:
            q= """
                with is_dup as
                (select distinct primary_key,table_name, 
                        case 
                            when rn > 1 then 'true'
                            when rn = 1 then 'false'
                        end as is_dup
                from
                (select '{}' as primary_key, SPLIT('{}', '.')[OFFSET(1)]  as table_name, row_number() over(partition by {}) rn
                from `{}`)

                ), 
                is_null as
                (select distinct '{}' as primary_key, SPLIT('{}', '.')[OFFSET(1)] as table_name,
                        case
                            when {} is null then 'true'
                            else 'false'
                        end as is_null
                from `{}`
                )
                select distinct t1.table_name, t1.primary_key, t1.is_null, t2.is_dup
                from is_null t1
                left join is_dup t2 on t1.table_name = t2.table_name and t1.primary_key = t2.primary_key
                """.format(col,tbl,col,tbl,col,tbl,col,tbl)
            df = load_df_from_bq(self.sa_creds,q)
            dfs.append(df)
        return dfs

    def test_not_null(self,table_list):
        dfs = self.transform(table_list)
        failed_rs = list()
        success_num = 0
        for df in dfs:
            if df.iloc[0]['is_null'] == 'true' and df.iloc[0]['table_name'] != 'employer_view_resume_data_mart':
                ts = ct.strftime("%Y-%m-%d %H:%M:%S")
                tbl_name = df.iloc[0]['table_name']
                col_name = df.iloc[0]['primary_key']
                failed_rs.append((tbl_name, col_name))
            else:
                success_num += 1
        total_num = success_num + len(failed_rs)
        return total_num, success_num, failed_rs
                
    def test_unique(self,table_list):
        dfs = self.transform(table_list)
        failed_rs = list()
        success_num = 0
        for df in dfs:
            if df.iloc[0]['is_dup'] == 'true' and df.iloc[0]['table_name'] != 'dwh_dim_company_region':
                ts = ct.strftime("%Y-%m-%d %H:%M:%S")
                tbl_name = df.iloc[0]['table_name']
                col_name = df.iloc[0]['primary_key']
                failed_rs.append((tbl_name, col_name))
            else:
                success_num += 1
        total_num = success_num + len(failed_rs)
        return total_num, success_num, failed_rs

    def execute(self):
        error = 0
        try:
            total_num, success_num, failed_rs = self.test_not_null(dds_list)
            ts = ct.strftime("%Y-%m-%d %H:%M:%S")
            if len(failed_rs):
                detail = "\n".join(map(lambda x: "\n  + Table {} column {} is null :no_entry_sign:".format(x[0], x[1]), failed_rs))
            else:
                detail = "PASS ALL TEST CASES"
            rule_type = "NOT NULL - DDS TABLES"
            msg = message_fmt.format(
                rule_type, ts, 
                "{}/{}".format(success_num, total_num), 
                "{}/{}".format(len(failed_rs), total_num), 
                detail
            )
            self.slack_client.chat_postMessage(channel= slack_channel, text = msg)
        except Exception as e:
            print(e)
            error += 1
            self.slack_client.chat_postMessage(channel= slack_channel, text = '+ not null test for dds tables error :no_entry_sign:')
            
        try:
            total_num, success_num, failed_rs = self.test_not_null(mart_list)
            ts = ct.strftime("%Y-%m-%d %H:%M:%S")
            if len(failed_rs):
                detail = "\n".join(map(lambda x: "\n  + Table {} column {} is null :no_entry_sign:".format(x[0], x[1]), failed_rs))
            else:
                detail = "PASS ALL TEST CASES"
            rule_type = "NOT NULL - DATA MART TABLES"
            msg = message_fmt.format(
                rule_type, ts, 
                "{}/{}".format(success_num, total_num), 
                "{}/{}".format(len(failed_rs), total_num), 
                detail
            )
            self.slack_client.chat_postMessage(channel= slack_channel, text = msg)
        except Exception as e:
            print(e)
            error += 1
            self.slack_client.chat_postMessage(channel= slack_channel, text = '+ not null test for data mart tables error :no_entry_sign:')

        try:
            total_num, success_num, failed_rs = self.test_unique(dds_list)
            ts = ct.strftime("%Y-%m-%d %H:%M:%S")
            if len(failed_rs):
                detail = "\n".join(map(lambda x: "\n  + Table {} column {} is null :no_entry_sign:".format(x[0], x[1]), failed_rs))
            else:
                detail = "PASS ALL TEST CASES"
            rule_type = "UNIQUE - DDS TABLES"
            msg = message_fmt.format(
                rule_type, ts, 
                "{}/{}".format(success_num, total_num), 
                "{}/{}".format(len(failed_rs), total_num), 
                detail
            )
            self.slack_client.chat_postMessage(channel= slack_channel, text = msg)
        except Exception as e:
            print(e)
            error += 1
            self.slack_client.chat_postMessage(channel= slack_channel, text = '+ unique test for dds tables error :no_entry_sign:')
        passed = 3 - error
        message = "Finished 3 unit_test with passed {}, error: {} at {} <@U0373UPED4L> :white_check_mark: ".format(passed, error, today)
        self.slack_client.chat_postMessage(channel= slack_channel, text =message)