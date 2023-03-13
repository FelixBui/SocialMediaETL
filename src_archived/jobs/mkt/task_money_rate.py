from core.app_base import AppBase
from datetime import timedelta
import pandas as pd
import requests
from libs.storage_utils import load_sa_creds
import pytz

import xml.etree.ElementTree as ET

vn_tz = pytz.timezone('Asia/Saigon')

class TaskMoneyRate(AppBase):
    def __init__(self, config):
        super().__init__(config)
        self.dp_ops = self.get_param_config(['dp-ops'])
        self.sa_creds = load_sa_creds(self.dp_ops)
        self.from_date = self.get_process_info(['from_date']).astimezone(vn_tz)
        self.to_date = self.get_process_info(['to_date']).astimezone(vn_tz)
        self.base_url = self.get_param_config(['base_url'])
        self.destination = self.get_param_config(['destination'])
        self.insert_mode = self.get_param_config(['insert_mode'])
        self.schema = self.get_param_config(['schema'])


    def extract(self):
        resp = requests.get(self.base_url).content
        return resp

    def transform(self, xml_string):
        root = ET.fromstring(xml_string)
        items = []
        for item in root.findall('./Exrate'):
            new = dict()
            new['cur_code'] = item.attrib['CurrencyCode'].lower()
            new['cur_name'] = item.attrib['CurrencyName'].strip().lower()
            new['Buy'] = item.attrib['Buy'].replace(',','')
            new['Transfer'] = item.attrib['Transfer'].replace(',','')
            new['Sell'] = item.attrib['Sell'].replace(',','')
            if new['Buy'] == '-':
                new['Buy'] = None
            if new['Transfer'] == '-':
                new['Transfer'] = None
            if new['Sell'] == '-':
                new['Sell'] = None
            items.append(new)
        items_df = pd.DataFrame(items)

        to_date = self.from_date + timedelta(days= 1)
        today_str = to_date.strftime("%Y-%m-%d")

        items_df['date_start'] = today_str
        items_df['date_start'] = pd.to_datetime(items_df['date_start']).dt.date
        items_df['Buy'] = items_df['Buy'].astype(float)
        items_df['Transfer'] = items_df['Transfer'].astype(float)
        items_df['Sell'] = items_df['Sell'].astype(float)
        items_df= items_df.rename(columns=str.lower)
        return items_df
    
    def load(self, df):
        df.to_gbq(
            self.destination, 
            if_exists= self.insert_mode, 
            table_schema= self.schema, 
            credentials= self.sa_creds)
    
    def execute(self):
        xml_str = self.extract()
        df = self.transform(xml_str)
        self.load(df)
      

    def backfill(self, interval=1):
        # from_date = self.from_date
        # to_date = self.to_date
        # seed_date = from_date
        # while seed_date <= to_date:
        #     self.execute()
        #     seed_date += timedelta(days=interval)
        #     self.from_date = seed_date
        pass
        


