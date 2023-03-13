#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
* Project: seiryu
* Author: thucpk
* Created: 2021/11/29
"""

from bson.codec_options import CodecOptions
from dateutil import tz

import pandas as pd
import tqdm
import datetime


from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from libs.storage_utils import load_df_from_mongo, load_df_from_postgres, insert_df_into_mongodb
from core.app_base import AppBase


local_tz = tz.tzlocal()
utc_tz = tz.tzutc()
codec_options = CodecOptions(tz_aware=True, tzinfo=local_tz)


class TaskCentralizeGetCompanyTaxid(AppBase):

    def __init__(self, config):
        super(TaskCentralizeGetCompanyTaxid, self).__init__(config)
        self.postgres_conf = self.get_param_config(['db', 'pg-competitor'])
        self.mongo_conf = self.get_param_config(['db', 'mg-competitor'])
        self.cx = self.get_param_config(['cx'])
        self.api_keys = self.get_param_config(['api_keys'])

    def extract(self):
        df_vnw_company = load_df_from_postgres(self.postgres_conf, "select id, name from vnw_company")
        df_topcv_company = load_df_from_postgres(self.postgres_conf, "select id, name from topcv_company")
        df_cb_company = load_df_from_postgres(self.postgres_conf, "select id, name from cb_company")
        df_itv_company = load_df_from_postgres(self.postgres_conf, "select id, name from itv_company")
        df_topdev_company = load_df_from_postgres(self.postgres_conf, "select id, name from topdev_company")
        df_tv365_company = load_df_from_postgres(self.postgres_conf, "select id, name from tv365_company")

        df_company_info = pd.concat([
            df_vnw_company[['id', 'name']],
            df_cb_company[['id', 'name']],
            df_topcv_company[['id', 'name']],
            df_itv_company[['id', 'name']],
            df_topdev_company[['id', 'name']],
            df_tv365_company[['id', 'name']]
        ])
        df_company_info['name'] = df_company_info['name'].str.lower()
        df_company_lookup_status = load_df_from_mongo(
            self.mongo_conf, 'company_lookup_status', query={}, selected_keys=['name'])
        company_names = set(df_company_info['name']) - set(df_company_lookup_status['name'])
        df_raw_company_lookup = pd.DataFrame(company_names, columns=['name'])
        df_raw_company_lookup['is_error'] = 0
        df_raw_company_lookup['is_lookup'] = 0
        df_raw_company_lookup['updated_at'] = datetime.datetime.now().replace(tzinfo=local_tz)
        if df_raw_company_lookup.shape[0] > 0:
            insert_df_into_mongodb(self.mongo_conf, 'company_lookup_status', df_raw_company_lookup, 'name')
        df_company_lookup_status = load_df_from_mongo(
            self.mongo_conf, 'company_lookup_status', query={"is_lookup": 0})
        return df_company_lookup_status

    @staticmethod
    def lookup_company_info(service, cx, company_name, debug=False):
        q = "ma so thue " + company_name
        res = service.cse().list(
            q=q,
            cx=cx,
        ).execute()
        rs = list()
        err = None
        rank = 1
        if res['searchInformation']['totalResults'] == '0':
            tax_info = {'original_name': company_name}
            rs.append(tax_info)
            return rs, {"query": q, "error": "search not found", "detail": res}

        for i in range(res['queries']['request'][0]['count']):
            try:
                name = res['items'][i]['pagemap'].get('person')
                url = res['items'][0]['link']
                data_source = res['items'][0]['displayLink']
                if name is None:
                    continue
                name = name[0]['name']
                org = res['items'][i]['pagemap'].get('organization')
                if org is None:
                    continue
                tax_info = org[0]
                tax_info['original_name'] = company_name
                tax_info['legal_representative'] = name
                tax_info['search_rank'] = rank
                tax_info['url'] = url
                tax_info['data_source'] = data_source
                rank += 1
                rs.append(tax_info)
            except Exception as e:
                err = e
        if err is None:
            if debug:
                return rs, {"query": q, "error": "success", "detail": res}
            return rs, {}
        else:
            return rs, {"query": q, "error": err, "detail": res}

    def transform(self, df_company_lookup_status):
        lst_err = list()
        lst_data = list()
        lst_index_processed = list()
        for project, key in self.api_keys.items():
            print("try key of ", project)
            service = build(
                "customsearch", "v1",
                developerKey=key
            )
            total_lookup = df_company_lookup_status[df_company_lookup_status['is_lookup'] == 0].shape[0]
            with tqdm.tqdm(total=total_lookup) as pbar:
                for ix in df_company_lookup_status[df_company_lookup_status['is_lookup'] == 0].index:
                    name = df_company_lookup_status.loc[ix, 'name']
                    try:
                        data, err = self.lookup_company_info(service, self.cx, name)
                        lst_data.extend(data)
                        lst_err.append(err)
                        df_company_lookup_status.loc[ix, 'is_lookup'] = 1
                        if len(err):
                            df_company_lookup_status.loc[ix, 'is_error'] = 1
                        pbar.update(1)
                        lst_index_processed.append(ix)
                    except HttpError as err:
                        if err.resp.status in [429]:
                            print('limit Queries per day')
                            break

        updated_at = datetime.datetime.now().replace(tzinfo=local_tz)
        df_company_lookup_status['updated_at'] = updated_at

        df_error = pd.DataFrame(lst_err)
        if df_error.shape[1] > 0:
            df_error['updated_at'] = updated_at
            df_error = df_error[~df_error['error'].isna()]
            df_error['name'] = df_error['query'].map(lambda x: x.replace('ma so thue ', ''))
            df_error['error'] = df_error['error'].map(lambda x: str(x).replace("'", ""))

        df_result = pd.DataFrame(lst_data)
        if df_result.shape[1] > 0:
            df_result['updated_at'] = updated_at
            df_result.loc[df_result['search_rank'].isna(), 'data_source'] = 'not_found'
            df_result.loc[df_result['search_rank'].isna(), 'search_rank'] = 0
            df_result.loc[df_result['search_rank'].isna(), 'name'] = ''

        return df_company_lookup_status, df_result, df_error

    def load(self, df_company_lookup_status, df_result, df_error):
        self.log.info("success: {}, error: {}".format(df_result.shape, df_error.shape))
        insert_df_into_mongodb(self.mongo_conf, 'company_lookup_result', df_result,
                               ['original_name', 'name', 'data_source', 'search_rank'])
        insert_df_into_mongodb(self.mongo_conf, 'company_lookup_error', df_error, 'name')
        insert_df_into_mongodb(self.mongo_conf, collection_name='company_lookup_status',
                               df=df_company_lookup_status, primary_key='name')

    def execute(self):
        self.log.info("step extract")
        df_company_lookup_status = self.extract()
        self.log.info("step transform")
        df_company_lookup_status, df_result, df_error = self.transform(df_company_lookup_status)
        self.log.info("step load")
        self.load(df_company_lookup_status, df_result, df_error)
