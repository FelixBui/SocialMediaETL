from core.app_base import AppBase
import pandas as pd
from datetime import timedelta
from time import sleep
from libs.utils import extract_utm_field_from_url
import requests
from sqlalchemy import create_engine
from scrapy import Selector
import re
from pprint import pprint
from libs.storage_utils import load_sa_creds, get_postgres_engine, load_df_from_postgres, insert_df_to_postgres
import pytz
from tqdm import tqdm
from time import sleep
import datetime

vn_tz = pytz.timezone('Asia/Saigon')


class TaskEnrichVnwCompany(AppBase):
    def __init__(self, config):
        super().__init__(config)
        self.dp_ops = self.get_param_config(["dp-ops"])
        self.sa_creds = load_sa_creds(self.dp_ops)
        self.postgres_conf = self.get_param_config(['db', 'postgres'])
        self.headers = {
            "content-type": "application/json",
            "Pragma": "no-cache",
            "Referer": "https://www.vietnamworks.com/",
            "X-Source": "Page-Container",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/96.0.4664.93 Safari/537.36 Edg/96.0.1054.53"
        }
        self.proxies = {
            "http": "http://torproxy-proxy3.torproxy:8118",
            "https": "http://torproxy-proxy3.torproxy:8118"
        }
        self.company_api = "https://ms.vietnamworks.com/company-profile/v1.0/company-profile/{}?vi"

    def get_seed_info(self):
        df = load_df_from_postgres(
            self.postgres_conf,
            """
                select j.company_id, j.url job_url
                from vnw_job_v2 j 
                left join vnw_company c on j.company_id = c.id 
                where c.id is null
            """
        )
        return df

    def get_company_info(self, job_url):
        response = requests.get(
            job_url,
            # proxies={'http': 'http://127.0.0.1:8118'},
            headers=self.headers)
        scrapy_response = Selector(text=response.text)
        if response.status_code != 200:
            print("Block")
            return None
        company_url = scrapy_response.css('.job-header-info .company-name a')
        if company_url:
            company_url = company_url.attrib['href']
        else:
            print("no info! " + str(job_url))
            return None
        extract_company_id = re.findall('-e\d+-', company_url)

        if extract_company_id:
            extract_company_id = extract_company_id[0].replace('-', '').replace('e', '')
        else:
            print("no info! " + str(job_url))
            return None
        print(extract_company_id)
        response = requests.get(
            self.company_api.format(extract_company_id),
            # proxies={'http': 'http://127.0.0.1:8118'},
            headers=self.headers)
        result = response.json()

        return result

    def get_company_created_at(self, company_id):
        df = load_df_from_postgres(
            self.postgres_conf,
            """
                select 
                    created_at
                from vnw_job_v2
                where company_id = '{}'
                order by created_at desc
            """.format(company_id)
        )
        if len(df) == 0:
            return None
        created_at = df['created_at'].to_list()[0]
        return created_at

    def extract(self):
        df = self.get_seed_info()
        job_url = df['job_url'].to_list()
        company_id = df['company_id'].to_list()
        company_info_lst = list()
        for index in tqdm(range(len(job_url))):
            company_info = self.get_company_info(job_url[index])
            if company_info is None:
                continue
            company_info = company_info['data']
            full_company_info = company_info['attributes']
            full_company_info['description'] = full_company_info['desc']
            full_company_info['address'] = full_company_info['location']
            full_company_info.update({'id': company_info['id']})
            full_company_info['created_at'] = self.get_company_created_at(company_id[index])
            full_company_info['updated_at'] = datetime.datetime.now()
            get_fields = ['id', 'name', 'contact', 'description', 'images', 'link', 'address', 'logo', 'size', 'videos',
                          'created_at', 'updated_at']
            final_company_info = dict()
            for el in get_fields:
                final_company_info[el] = full_company_info[el]
            company_info_lst.append(final_company_info)
            sleep(1)

        company_info_df = pd.DataFrame(company_info_lst)
        return company_info_df

    @staticmethod
    def transform(df):
        return df

    def load(self, input_df):
        insert_df_to_postgres(
            self.postgres_conf, tbl_name="vnw_company",
            df=input_df,
            primary_keys=["id"])

    def execute(self):
        df = self.extract()
        self.load(df)
