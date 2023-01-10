#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
* Project: seiryu
* Author: thucpk
* Created: 2021/11/29
"""

import pandas as pd
import tqdm
import requests
from core.app_base import AppBase
from dateutil import tz
import scrapy
import time
import random

from libs.storage_utils import load_df_from_postgres, insert_df_to_postgres

local_tz = tz.tzlocal()


class TaskCentralizeVnwCompanySize(AppBase):

    map_size_name = {
        0: "Không xác định",
        1: "Dưới 20 người",
        2: "20 - 150 người",
        3: "150 - 300 người",
        4: "Trên 300 người"
    }

    def __init__(self, config):
        super(TaskCentralizeVnwCompanySize, self).__init__(config)
        self.postgres_conf = self.get_param_config(['db', 'pg-competitor'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

    @staticmethod
    def get_vnw_size_min_max(x, pos=0):
        if x is None:
            return None
        x = x.replace('.', '').replace(',', '').replace(
            'Hơn 50000', '50000-999999').replace("Ít hơn 10", '0-9').split('-')
        if len(x) > 1:
            return int(x[pos])

    @staticmethod
    def norm_size(x_max):
        range_size = [20, 150, 300]
        if (x_max is None) or pd.isna(x_max):
            return 0
        for ix, v in enumerate(range_size):
            if x_max < v:
                return ix + 1
        else:
            return len(range_size) + 1

    def extract(self):
        raw_df = load_df_from_postgres(
            postgres_conf=self.postgres_conf,
            query="""
        select j.id job_id, j.company_id id, j.url job_url, j.created_at job_created_at
        from vnw_job_v2 j
        left join vnw_company_size s on j.company_id = s.id
        where s.id is null
        order by j.created_at desc
        """).sort_values(['job_created_at'], ascending=False).drop_duplicates(['id'])[['id', 'job_url']]
        return raw_df

    @classmethod
    def transform(cls, vnw_lookup_df):
        headers = {
            "Content-Type": "application/json",
            "sec-ch-ua-mobile": "?0",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/95.0.4638.69 Safari/537.36 Edg/95.0.1020.44",
            "Accept": "*/*",
            "Origin": "https://www.vietnamworks.com",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://www.vietnamworks.com/"
        }
        proxies = {
            "http": "http://torproxy-proxy3.torproxy:8118",
            "https": "http://torproxy-proxy3.torproxy:8118"
        }
        rs = []
        for url in tqdm.tqdm(vnw_lookup_df['job_url']):
            v = ''
            t = requests.get(url, proxies=proxies, headers=headers)
            if t.status_code == 406:
                continue
            tmp = scrapy.Selector(text=t.text).xpath("//div[@class='col-xs-10 summary-content']")
            if len(tmp):
                for e in tmp:
                    if e.xpath("./span[@class='content-label']/text()").get() == 'Quy Mô Công Ty':
                        v = e.xpath("./span[@class='content']/text()").get()
            rs.append(v)
            time.sleep(random.uniform(0.1, 0.5))
        vnw_lookup_df = vnw_lookup_df.reset_index(drop=True)
        vnw_lookup_df.loc[:len(rs)-1, 'size'] = rs
        vnw_lookup_df = vnw_lookup_df.dropna()
        vnw_lookup_df['size'] = vnw_lookup_df['size'].map(lambda x: x.replace(' nhân viên', ''))
        vnw_lookup_df['size_min'] = vnw_lookup_df['size'].map(lambda x: cls.get_vnw_size_min_max(x, 0))
        vnw_lookup_df['size_max'] = vnw_lookup_df['size'].map(lambda x: cls.get_vnw_size_min_max(x, 1))
        vnw_lookup_df['size_code'] = vnw_lookup_df['size_max'].map(cls.norm_size)
        vnw_lookup_df['size_name'] = vnw_lookup_df['size_code'].map(cls.map_size_name)
        return vnw_lookup_df

    def load(self, df):
        insert_df_to_postgres(
            self.postgres_conf, tbl_name="vnw_company_size",
            df=df[['id', 'size', 'size_min', 'size_max', 'size_code', 'size_name']],
            primary_keys=["id"])

    def execute(self):
        vnw_lookup_df = self.extract()
        df = self.transform(vnw_lookup_df)
        self.load(df)
