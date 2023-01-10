import numpy as np

from dateutil import tz
import datetime

from core.app_base import AppBase
import base64
from libs.storage_utils import load_df_from_mysql, load_sa_creds, get_bq_cli, load_df_from_bq

local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class MatomoBase(AppBase):

    def __init__(self, config):
        super(MatomoBase, self).__init__(config)
        self.from_date = (self.get_process_info(['from_date']) - datetime.timedelta(hours=1)).replace(minute=0)
        self.to_date = (self.get_process_info(['to_date']) - datetime.timedelta(hours=1)).replace(minute=0)
        self.mysql_conf = self.get_param_config(['db', 'mysql_matomo'])
        self.sql_template = self.get_param_config(['sql_template'], option=True)
        self.gcp_sv_data_account_key_path = self.get_param_config(['gcp_sv_data_account_key_path'])
        self.bq_creds = load_sa_creds(self.gcp_sv_data_account_key_path)
        self.tbl_name = self.get_param_config(['tbl_name'])
        self.write_mode = self.get_param_config(['write_mode'])
        self.time_event_column = self.get_param_config(['time_event_column'], option=True)

    def query_builder(self):
        return self.sql_template.format(self.from_date, self.to_date)

    def extract(self):
        q = self.query_builder()
        print(q)
        df = load_df_from_mysql(self.mysql_conf, q, 'matomo')
        return df

    def load(self, df):
        bq_cli = get_bq_cli(self.gcp_sv_data_account_key_path)
        dataset_id, table_id = self.tbl_name.split('.')
        q = """SELECT COUNT(1) AS cnt
            FROM `{0}.__TABLES_SUMMARY__`
            WHERE table_id = '{1}' """.format(dataset_id, table_id)
        df_check = load_df_from_bq(self.bq_creds, q)
        if df_check.iloc[0]['cnt'] > 0:
            dataset_ref = bq_cli.dataset(dataset_id, project='data-warehouse-sv')
            table_ref = dataset_ref.table(table_id)
            table = bq_cli.get_table(table_ref)  # API Request
            schema = table.schema
            _schema = []
            for field in schema:
                _schema.append({'name': field.name, 'type': field.field_type})
        else:
            _schema = None

        if self.write_mode == "upsert_time_event":
            if df.shape[0] > 0:
                if isinstance(df[self.time_event_column].iloc[0], (int, float, np.int64, np.float64)):
                    q = """delete from {0} where {1} >= {2} and {1} <= {3} """.format(
                        self.tbl_name, self.time_event_column, df[self.time_event_column].min(),
                        df[self.time_event_column].max()
                    )
                else:
                    q = """delete from {0} where {1} >= '{2}' and {1} <= '{3}' """.format(
                        self.tbl_name, self.time_event_column, df[self.time_event_column].min(),
                        df[self.time_event_column].max()
                    )
                print(q)
                j = bq_cli.query(q)
                j.result()
                df.to_gbq("{}".format(self.tbl_name),
                          if_exists="append", credentials=self.bq_creds, table_schema=_schema)
        elif self.write_mode == "append":
            df.to_gbq("{}".format(self.tbl_name), if_exists="append", credentials=self.bq_creds, table_schema=_schema)
        elif self.write_mode in ("overwrite", "replace"):
            df.to_gbq("{}".format(self.tbl_name), if_exists="replace", credentials=self.bq_creds)
        else:
            raise Exception("write mode: {} is not support".format(self.write_mode))

    @staticmethod
    def transform(raw_df):
        df = raw_df.applymap(lambda x: base64.b64encode(x).decode('utf-8') if isinstance(x, bytes) else x)
        return df

    @AppBase.wrapper_simple_log
    def execute(self):
        raw_df = self.extract()
        df = self.transform(raw_df)
        self.load(df)
