import tqdm
import datetime
from dateutil import tz
from dateutil.parser import parse as parse_time

from core.app_base import AppBase
from libs.storage_utils import down_gcs_to_df, get_gcs_cli, \
    insert_df_to_postgres, get_rmq_cli, get_rmq_channel, consume_rmq_channel

local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class TaskJaSaveResultQc(AppBase):

    def __init__(self, config):
        super(TaskJaSaveResultQc, self).__init__(config)
        env_name = self.get_param_config(['env_name'])
        db_key = {
            "prod": ["db", "pg-dp-services"],
            "stag": ["db", "pg-dp-stag"],
            "qc": ["db", "pg-dp-qc"]
        }.get(env_name)
        self.postgres_conf = self.get_param_config(db_key)
        dp_ops = self.get_param_config(['dp-ops'])
        self.gcs_cli = get_gcs_cli(dp_ops)
        self.rmq_conf = self.get_param_config(['db', 'rb-rabbitmq'])
        self.rmq_channel_name = self.get_param_config(['rmq_channel_name'])
        self.from_date = self.get_process_info(['from_date']) + datetime.timedelta(hours=6)
        self.to_date = self.get_process_info(['to_date']) + datetime.timedelta(hours=6)

    def extract(self):
        with tqdm.tqdm() as pbar:
            self.rmq_conf['heartbeat'] = 1200
            self.rmq_conf['blocked_connection_timeout'] = 900
            rmq_cli = get_rmq_cli(self.rmq_conf)
            rmq_channel = get_rmq_channel(rmq_cli, self.rmq_channel_name)
            for msg in consume_rmq_channel(rmq_channel, self.rmq_channel_name, inactivity_timeout=1200):
                bucket_name = msg['bucket_name']
                object_key = msg['object_key']
                df = down_gcs_to_df(self.gcs_cli, bucket_name, object_key)
                self.log.info("in path: {}, {}".format(object_key, df.shape))
                df['resume_id'] = df['resume_id'].astype('str')
                df['job_id'] = df['job_id'].astype('str')
                df['seeker_id'] = df['seeker_id'].astype('str')
                # df['updated_at'] = df['updated_at'].map(parse_time)
                insert_df_to_postgres(
                    self.postgres_conf,
                    tbl_name="ja_scoring",
                    df=df,
                    primary_keys=['job_id', 'resume_id']
                )
                pbar.update(df.shape[0])

    @AppBase.wrapper_simple_log
    def execute(self):
        self.extract()
