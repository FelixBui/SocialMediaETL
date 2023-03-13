from core.app_base import AppBase

import pandas as pd
from datetime import timedelta
from time import sleep
from libs.utils import extract_utm_field_from_url
import requests
from libs.storage_utils import load_sa_creds
import pytz

vn_tz = pytz.timezone('Asia/Saigon')


class TaskTiktokDim(AppBase):
    def __init__(self, config):
        super().__init__(config)
        self.dp_ops = self.get_param_config(["dp-ops"])
        self.sa_creds = load_sa_creds(self.dp_ops)
        self.tiktok_auth = self.get_param_config(["mkt-auth", "tiktok"])
        self.from_date = self.get_process_info(["from_date"]).astimezone(vn_tz)
        self.to_date = self.get_process_info(["to_date"]).astimezone(vn_tz)
        self.base_url = self.get_param_config(["base_url"])
        self.dim_parameter = self.get_param_config(["dim_parameter"])
        self.bq_load_info = self.get_param_config(["bq_load_info"])

    # change property name to standard name schema
    @staticmethod
    def mapping_ad_info_to_base_prop(df):
        df.rename(
            columns={
                "ad_name": "name",
                "ad_id": "id",
            },
            inplace=True,
        )
        return df

    # change property name to standard name schema
    @staticmethod
    def mapping_adgroup_info_to_base_prop(df):
        df.rename(
            columns={
                "adgroup_name": "name",
                "adgroup_id": "id",
            },
            inplace=True,
        )
        return df

    # change property name to standard name schema
    @staticmethod
    def mapping_campaign_info_to_base_prop(df):
        df.rename(
            columns={
                "campaign_name": "name",
                "campaign_id": "id",
                "create_time": "created_time",
                "modify_time": "updated_time",
            },
            inplace=True,
        )
        return df

    # to get ad, adgroup, campaign info with one func
    def get_dim_info(self, fields_lst_str, channel_name, level):
        url = """
            {}/{}/get/?advertiser_id={}&fields={}&page_size=1000
        """.format(
            self.base_url,
            level,
            self.tiktok_auth[channel_name]["advertiser_id"],
            fields_lst_str,
        )
        response = requests.get(
            url,
            headers={
                "Content-Type": "application/json",
                "Access-Token": "{}".format(
                    self.tiktok_auth[channel_name]["access_token"]
                ),
            },
        )
        response = response.json()
        rs = response["data"]["list"]
        all_info_lst = list()
        for rc in rs:
            all_info_lst.append(rc)
        return all_info_lst

    def extract_ad_dim(self, ad_dim_fields_lst_str, channel_name):
        rs = self.get_dim_info(ad_dim_fields_lst_str, channel_name, "ad")
        df = pd.DataFrame(rs)
        return df

    def transform_ad_dim(self, df, channel_name):
        df.rename(columns={"landing_page_url": "utm_url"}, inplace=True)
        df = self.mapping_ad_info_to_base_prop(df)
        df.adgroup_id = df.adgroup_id.astype(str)
        df.id = df.id.astype(str)
        df["utm_campaign"] = df["utm_url"].apply(
            lambda x: extract_utm_field_from_url(x, "campaign")
        )
        df["utm_source"] = df["utm_url"].apply(
            lambda x: extract_utm_field_from_url(x, "source")
        )
        df["utm_medium"] = df["utm_url"].apply(
            lambda x: extract_utm_field_from_url(x, "medium")
        )
        df["channel_name"] = channel_name
        df["mkt_channel"] = "Tiktok"
        df = df[
            [
                "adgroup_id",
                "id",
                "name",
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_url",
                "channel_name",
                "mkt_channel",
            ]
        ]
        return df

    def extract_adgroup_dim(self, adgroup_dim_fields, channel_name):
        rs = self.get_dim_info(adgroup_dim_fields, channel_name, "adgroup")
        df = pd.DataFrame(rs)
        return df

    def transform_adgroup_dim(self, df, channel_name):
        df = self.mapping_adgroup_info_to_base_prop(df)
        df.campaign_id = df.campaign_id.astype(str)
        df.id = df.id.astype(str)
        df["channel_name"] = channel_name
        df["mkt_channel"] = "Tiktok"
        df = df[["campaign_id", "id", "name", "channel_name", "mkt_channel"]]
        return df

    def extract_campaign_dim(self, campaign_dim_fields, channel_name):
        rs = self.get_dim_info(campaign_dim_fields, channel_name, "campaign")
        df = pd.DataFrame(rs)
        return df

    def transform_campaign_dim(self, df, channel_name):
        df = self.mapping_campaign_info_to_base_prop(df)
        df.id = df.id.astype(str)
        df["channel_name"] = channel_name
        df["mkt_channel"] = "Tiktok"
        df["created_time"] = pd.to_datetime(df["created_time"]).dt.date
        df["updated_time"] = pd.to_datetime(df["updated_time"]).dt.date
        df = df[
            [
                "id",
                "name",
                "objective",
                "created_time",
                "updated_time",
                "channel_name",
                "mkt_channel",
            ]
        ]
        return df

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
        # for each time excute -> ETL ad, adgroup, campaign dim
        # now just only vl24h channel

        # try:
        df = self.extract_ad_dim(
            self.dim_parameter["ad_dim_fields_lst_str"], "Vieclam24h"
        )
        df = self.transform_ad_dim(df, "Vieclam24h")
        self.load(df, "ad")

        # adgroup
        sleep(3)
        df = self.extract_adgroup_dim(
            self.dim_parameter["adgroup_dim_fields_lst_str"], "Vieclam24h"
        )
        df = self.transform_adgroup_dim(df, "Vieclam24h")
        self.load(df, "adgroup")

        # campaign
        sleep(3)
        df = self.extract_campaign_dim(
            self.dim_parameter["campaign_dim_fields_lst_str"], "Vieclam24h"
        )
        df = self.transform_campaign_dim(df, "Vieclam24h")
        self.load(df, "campaign")

        # except Exception as e:
        #     my_bot = slack.SlackBot(self.slack_config["bot_url"])
        #     my_bot.send_msg(
        #         self.slack_config["message"] + "===== Detail: " + str(e),
        #         self.slack_config["message_block"],
        #     )

    def backfill(self, interval=1):
        from_date = self.from_date
        to_date = self.to_date
        seed_date = from_date
        while seed_date <= to_date:
            self.execute()
            seed_date += timedelta(days=interval)
            self.from_date = seed_date
