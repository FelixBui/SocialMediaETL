from dateutil import tz
from core.app_base import AppBase

import pandas as pd
from datetime import timedelta
from time import sleep
from libs.storage_utils import load_sa_creds
import requests
import pytz

vn_tz = pytz.timezone('Asia/Saigon')


class TaskTiktokFact(AppBase):
    def __init__(self, config):
        super().__init__(config)
        self.dp_ops = self.get_param_config(["dp-ops"])
        self.sa_creds = load_sa_creds(self.dp_ops)
        self.channel_config = self.get_param_config(["mkt-auth", "tiktok"])
        self.from_date = self.get_process_info(["from_date"]).astimezone(vn_tz)
        self.to_date = self.get_process_info(["to_date"]).astimezone(vn_tz)
        self.base_url = self.get_param_config(["base_url"])
        self.fact_parameter = self.get_param_config(["fact_parameter"])
        self.bq_load_info = self.get_param_config(["bq_load_info"])
        self.is_backfill = False
        self.days_to_subtract = self.get_param_config(['days_to_subtract'])

    @staticmethod
    def convert_to_df(res_lst):
        rs_lst = list()
        for rc in res_lst:
            tmp_dict = dict()
            for key in rc["dimensions"]:
                tmp_dict[key] = rc["dimensions"][key]
            for key in rc["metrics"]:
                tmp_dict[key] = rc["metrics"][key]
            rs_lst.append(tmp_dict)
        return pd.DataFrame(rs_lst)

    def get_report(
        self,
        start_date,
        end_date,
        dimension_lst_str,
        metric_lst_str,
        data_level,
        channel_name,
    ):
        # Get report with custom dimension or metric with specific data level
        # data level: AUCTION_CAMPAIGN - AUCTION_ADGROUP - AUCTION_AD - AUCTION_ADVERTISER
        # dimesions: campaign_id - adgroup_id - ad_id - advertiser_id
        # metrics: clicks - impressions - spend
        url = "{}/reports/integrated/get/?advertiser_id={}&service_type=AUCTION&report_type=BASIC" \
              "&data_level={}&dimensions={}&metrics={}&start_date={}&end_date={}" \
              "&lifetime=false&page_size=1000".format(
                self.base_url, self.channel_config[channel_name]["advertiser_id"],
                data_level,
                dimension_lst_str,
                metric_lst_str,
                start_date,
                end_date)
        response = requests.get(
            url,
            headers={
                "Content-Type": "application/json",
                "Access-Token": "{}".format(
                    self.channel_config[channel_name]["access_token"]
                ),
            },
        )
        response = response.json()
        return response

    def get_fact(self, date, dimension_lst_str, metric_lst_str, level, channel_name):
        mapping_api_level = {
            "ad": "AUCTION_AD",
            "adgroup": "AUCTION_ADGROUP",
            "campaign": "AUCTION_CAMPAIGN",
        }
        rs = self.get_report(
            date,
            date,
            dimension_lst_str,
            metric_lst_str,
            mapping_api_level[level],
            channel_name,
        )
        return rs

    def extract_ad_fact(self, date):
        rs = self.get_fact(
            date,
            '["ad_id"]',
            self.fact_parameter["ad_fact_fields_lst_str"],
            "ad",
            "Vieclam24h",
        )
        df = self.convert_to_df(rs["data"]["list"])
        return df

    def extract_adgroup_fact(self, date):
        rs = self.get_fact(
            date,
            '["adgroup_id"]',
            self.fact_parameter["adgroup_fact_fields_lst_str"],
            "adgroup",
            "Vieclam24h",
        )
        df = self.convert_to_df(rs["data"]["list"])
        return df

    def extract_campaign_fact(self, date):
        rs = self.get_fact(
            date,
            '["campaign_id"]',
            self.fact_parameter["campaign_fact_fields_lst_str"],
            "campaign",
            "Vieclam24h",
        )
        df = self.convert_to_df(rs["data"]["list"])
        return df

    # this version transform use for all (ad, adgr, cam) because same metrics
    @staticmethod
    def transform(df, date, level):
        level_mapping = {
            "ad": "ad_id",
            "adgroup": "adgroup_id",
            "campaign": "campaign_id",
        }
        df["currency_unit"] = pd.Series(["VND" for _ in range(df.shape[0])])
        df["date_start"] = pd.Series([date for _ in range(df.shape[0])])
        df["spend"] = df["spend"].astype(float)
        df["clicks"] = df["clicks"].astype(int)
        df["reach"] = df["reach"].astype(int)
        df["impressions"] = df["impressions"].astype(int)
        df["frequency"] = df["frequency"].astype(float)
        df[level_mapping[level]] = df[level_mapping[level]].astype(str)
        df = df[
            (df["spend"] > 0)
            | (df["clicks"] > 0)
            | (df["frequency"] > 0)
            | (df["reach"] > 0)
            | (df["impressions"] > 0)
        ]
        df.reset_index(inplace=True)
        return df[
            [
                "{}".format(level_mapping[level]),
                "clicks",
                "frequency",
                "impressions",
                "reach",
                "spend",
                "date_start",
                "currency_unit",
            ]
        ]

    def load(self, df, level):
        destination = self.bq_load_info[level]["destination"]
        insert_mode = self.bq_load_info[level]["insert_mode"]
        schema = self.bq_load_info[level]["schema"]
        print(df.shape)
        df.to_gbq(
            destination,
            if_exists=insert_mode,
            table_schema=schema,
            credentials=self.sa_creds,
        )

    def execute(self):
        date = self.from_date
        if not self.is_backfill:
            date = self.from_date - timedelta(days= self.days_to_subtract)
        date_str = date.strftime("%Y-%m-%d")
        # ad
        df = self.extract_ad_fact(date_str)
        df = self.transform(df, date_str, "ad")
        self.load(df, "ad")

        # adgroup
        # df = self.extract_adgroup_fact(date_str)
        # df = self.transform(df, date_str, "adgroup")
        # self.load(df, "adgroup")

        # campaign
        # df = self.extract_campaign_fact(date_str)
        # df = self.transform(df, date_str, "campaign")
        # self.load(df, "campaign")

    def backfill(self, interval=1):
        self.is_backfill = True
        from_date = self.from_date
        to_date = self.to_date
        seed_date = from_date
        while seed_date <= to_date:
            print()
            print(seed_date)
            self.execute()
            seed_date += timedelta(days=interval)
            self.from_date = seed_date
            sleep(5)
