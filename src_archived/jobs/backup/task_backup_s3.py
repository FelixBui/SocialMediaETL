import datetime

from core.app_base import AppBase
import pandas as pd
import tqdm
import os
import re
from dateutil import tz
import itertools
import abc
import requests
import time
import random

from libs.storage_utils import insert_df_to_postgres, get_boto3_s3_client, upload_file_to_s3
from libs.network_utils import get_dict_proxy, requests_retry_session
from libs.utils import mkdir_dir, make_tarfile

local_tz = tz.tzlocal()
remove_non_alphabet = re.compile('[^a-zA-Z ]')


class TaskBackupS3(AppBase):

    def __init__(self, config):
        super(TaskBackupS3, self).__init__(config)
        self.postgres_conf = self.get_param_config(['db', 'postgres'])
        aws_certs_conf = self.get_param_config(['aws_certs'])
        self.s3_template = self.get_param_config(['s3_template'])
        self.boto_s3_cli = get_boto3_s3_client(aws_certs_conf)
        self.from_date = self.get_process_info(['from_date']) - datetime.timedelta(days=1)
        self.to_date = self.get_process_info(['to_date']) - datetime.timedelta(days=1)
        self.proxies_conf = self.get_param_config(['proxies_conf'])

    @abc.abstractmethod
    def extract(self):
        pass

    @classmethod
    def try_proxy(cls, url, proxies, loop=0):
        try:
            if loop < len(proxies):
                dct_proxy = proxies[loop]
                r = requests_retry_session(retries=5, backoff_factor=10).get(url, proxies=dct_proxy)
                return r
            else:
                raise Exception("web scraping blocked")
        except requests.exceptions.ProxyError:
            cls.try_proxy(url, proxies, loop+1)

    def crawl_to_backup(self, df_full):
        proxies = [get_dict_proxy(c) for c in self.proxies_conf]
        # dct_proxy = get_dict_proxy(self.tor_proxy_conf)
        in_format = self.s3_template['in_format']  # "/tmp/{0}/{1}/{2}/{3}.html"
        s3_in_format = self.s3_template['s3_in_format']  # "{0}/{1}/{2}"
        out_tar_format = self.s3_template['out_tar_format']  # "{}.tar.gz"
        lst_rs = list()
        set_ds = set(df_full['ds'])
        set_sourcetype = set(df_full['sourcetype'])
        set_channel_code = set(df_full['channel_code'])
        with tqdm.tqdm(total=df_full.shape[0]) as pbar:
            for ds, sourcetype, channel_code in list(itertools.product(set_ds, set_sourcetype, set_channel_code)):
                for _, r in df_full.loc[
                    (df_full['ds'] == ds) &
                    (df_full['sourcetype'] == sourcetype) &
                    (df_full['channel_code'] == channel_code)
                ].iterrows():
                    time.sleep(random.uniform(5, 10))
                    _id = r['id']
                    url = r['url']
                    channel_code = r['channel_code']
                    sourcetype = r['sourcetype']
                    ds = r['ds']
                    in_path = in_format.format(channel_code, sourcetype, ds, _id)
                    res = self.try_proxy(url, proxies)
                    html = res.text
                    in_dir = os.path.dirname(in_path)
                    mkdir_dir(in_dir)
                    out_path = out_tar_format.format(channel_code, sourcetype, ds)
                    out_dir = os.path.dirname(out_path)
                    mkdir_dir(out_dir)
                    out_file = os.path.basename(out_path)
                    s3_path = s3_in_format.format(channel_code, sourcetype, out_file)

                    with open(in_path, 'w+') as f:
                        f.write(html)

                    result = {
                        "channel_code": channel_code,
                        "sourcetype": sourcetype,
                        "id": _id,
                        "status_code": res.status_code,
                        "size": len(html),
                        "ds": ds,
                        "s3_path": s3_path
                    }
                    lst_rs.append(result)
                    pbar.update(1)

                print(in_dir, out_path, s3_path)
                make_tarfile(in_path=in_dir, out_path=out_path)
                upload_file_to_s3(self.boto_s3_cli,
                                  obj=out_path,
                                  bucket=self.s3_template['bucket'],
                                  object_key=s3_path,
                                  use_obj=False)

        return pd.DataFrame(lst_rs)

    def transform(self, df_full):
        df_full['ds'] = df_full['created_at'].map(lambda x: str(x.date()))
        return self.crawl_to_backup(df_full)

    def load(self, df_backup_s3_tracking):
        insert_df_to_postgres(
            self.postgres_conf, tbl_name="backup_s3_tracking",
            df=df_backup_s3_tracking, primary_keys=['channel_code', 'sourcetype', 'id'])

    def execute(self):
        self.log.info("step extract")
        df_full = self.extract()
        df_backup_s3_tracking = self.transform(df_full)

        self.load(df_backup_s3_tracking)
