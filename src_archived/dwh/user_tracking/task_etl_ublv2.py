import datetime
from dateutil import tz
import io
import tqdm
import pandas as pd
from core.app_base import AppBase
import re
import ast
import pyarrow.orc
from libs.storage_utils import get_boto3_s3_client, list_object_from_s3, down_file_from_s3, \
    load_sa_creds, get_bq_cli

local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class TaskEtlUblv2(AppBase):

    def __init__(self, config):
        super(TaskEtlUblv2, self).__init__(config)
        self.from_date = (self.get_process_info(['from_date']) - datetime.timedelta(hours=1)).replace(minute=0)
        self.to_date = (self.get_process_info(['to_date']) - datetime.timedelta(hours=1)).replace(minute=0)
        aws_certs_conf = self.get_param_config(['aws_certs'])
        self.boto_s3_cli = get_boto3_s3_client(aws_certs_conf)
        self.s3_template = self.get_param_config(['s3_template'])
        self.gcp_sv_data_account_key_path = self.get_param_config(['gcp_sv_data_account_key_path'])
        self.bq_creds = load_sa_creds(self.gcp_sv_data_account_key_path)
        self.events = self.get_param_config(['events'])
        self.prefix_key = self.get_param_config(['prefix_key'])
        self. metadata_df = pd.read_csv(self.get_param_config(['ublv2_metadata']),
                                        usecols=['name', 'regex', 'channel_code'])
        self.button_df = pd.read_csv(self.get_param_config(['ublv2_button']),
                                     usecols=['name', 'regex', 'channel_code'])

    @staticmethod
    def parse_path_url(path_url, channel_code, metadata_dict_full):
        if channel_code not in metadata_dict_full:
            return 'no_info', path_url
        for key, value in metadata_dict_full[channel_code].items():
            if re.search(key, path_url):
                sub_url = ""
                tmp = path_url.split('/')
                #             '/cv/abc/456/12345 -> sub_url = '/abc/456/12345'
                if len(tmp) > 2:
                    sub_url = sub_url.join(["/" + el for el in tmp[2:]])
                return metadata_dict_full[channel_code][key], sub_url
        return 'other', path_url

    @staticmethod
    def parse_el(element, channel_code, button_dict_full):
        if channel_code not in button_dict_full:
            return None
        for key, value in button_dict_full[channel_code].items():
            if re.search(key, element):
                return button_dict_full[channel_code][key]
        return None

    def is_event_match(self, x):
        for event in self.events:
            if '{}/{}'.format(self.prefix_key, event) in x:
                return True
        return False

    def filter_dt(self, x):
        _t = datetime.datetime.strptime(
            '/'.join(x.split('/')[-2:]), '%Y-%m-%d/%H00.orc'
        ).replace(tzinfo=utc_tz)
        if (_t >= self.from_date) and (_t < self.to_date):
            return True
        else:
            return False

    def extract(self):
        tmp = list()
        for e in list_object_from_s3(self.boto_s3_cli, bucket=self.s3_template['bucket'], prefix=self.prefix_key):
            tmp.append(e)
        df_s3 = pd.DataFrame(tmp)
        # filter event view & click
        df_s3 = df_s3.loc[df_s3['Key'].map(self.is_event_match) & df_s3['Key'].map(self.filter_dt)]
        return df_s3

    def transform_df(self, input_df):
        # Load metadata
        metadata_df = self.metadata_df
        # Clean and Transform data
        metadata_df = metadata_df.fillna({'regex': 'no_info'})
        metadata_df['channel_code'] = metadata_df['channel_code'].str.upper()

        # convert df to dict group by channel_code === ex:
        # {'tvn':{'^/$':'home_page','tuyen-':'job_page'},'vl24h':{'^/$':'home_page'}}
        metadata_dict_full = {}
        for index, row in metadata_df.iterrows():
            if row['channel_code'] not in metadata_dict_full:
                channel_code = row['channel_code']
                metadata_dict_full[channel_code] = {}
            metadata_dict_full[channel_code][row['regex']] = row['name']

        # Clean data
        input_df = input_df.applymap(lambda x: x.replace("\x00", '') if isinstance(x, str) and "\x00" in x else x)
        # sst.literal_eval to convert <class: str> to dict 
        input_df['source_info'] = input_df['source_info'].apply(lambda x: ast.literal_eval(x))
        # path_url 
        input_df['path_url'] = input_df['source_info'].map(lambda x: x['path_url'])
        # url to page
        input_df['page'] = input_df.apply(
            lambda x: self.parse_path_url(x.path_url, x.channel_code, metadata_dict_full)[0],
            axis=1
        )
        # url to subpage
        input_df['sub_page'] = input_df.apply(
            lambda x: self.parse_path_url(x.path_url, x.channel_code, metadata_dict_full)[1],
            axis=1
        )

        # load button_match_rule

        # Clean and Transform data
        button_df = self.button_df.fillna({'regex': 'no_info'})
        button_df['channel_code'] = button_df['channel_code'].str.upper()

        # convert df-> dict group by channel_code =
        # ex: {'tvn':{'btn-[a-z-]*apply':'apply','btn-[a-z-]*search':'search'},'vl24h':{}}
        button_dict_full = {}
        for index, row in button_df.iterrows():
            if row['channel_code'] not in button_dict_full:
                channel_code = row['channel_code']
                button_dict_full[channel_code] = {}
            button_dict_full[channel_code][row['regex']] = row['name']

        # Normal element
        input_df['norm_el'] = input_df['el'].str.split('>').map(lambda x: ">".join(x[-3:]))
        # El to button
        input_df['button_name'] = input_df.apply(
            lambda x: self.parse_el(x.norm_el, x.channel_code, button_dict_full),
            axis=1)
        return input_df

    def transform(self, df_s3):
        # downfile
        lst_dfs = list()
        for _, r in tqdm.tqdm(df_s3.iterrows()):
            key = r['Key']
            obj = io.BytesIO()
            down_file_from_s3(
                self.boto_s3_cli,
                bucket="sv-data-lake",
                object_name=key,
                obj=obj,
                use_obj=True,
            )
            _df = pd.read_orc(obj)
            lst_dfs.append(_df)

        df = pd.concat(lst_dfs, ignore_index=True)
        df = self.transform_df(df)
        return df

    def load(self, df):
        bq_cli = get_bq_cli(self.gcp_sv_data_account_key_path)
        for event in self.events:
            df_sub = df[df['event'].str.startswith(event)]
            if df_sub.shape[0] > 0:
                q = """delete from ublv2.{} where created_at >='{}' and created_at <= '{}' """.format(
                    event, df_sub['created_at'].min(), df_sub['created_at'].max()
                )
                j = bq_cli.query(q)
                j.result()
                df_sub.to_gbq("ublv2.{}".format(event), if_exists="append", credentials=self.bq_creds)

    def execute(self):
        # step 1 - extract
        self.log.info("# step 1: extract")
        # return list of dataframe 
        df_s3 = self.extract()

        # step 2 - transform
        self.log.info("# step 2: transform")
        df = self.transform(df_s3)

        # step 3 - load
        self.log.info("# step 3: load")
        self.load(df)
