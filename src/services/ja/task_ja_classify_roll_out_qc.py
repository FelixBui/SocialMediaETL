from dateutil import tz
import datetime

from core.app_base import AppBase
from libs.storage_utils import load_df_from_mysql, insert_df_to_postgres, load_df_from_postgres

local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class TaskJaClassifyRollOutQc(AppBase):

    def __init__(self, config):
        super(TaskJaClassifyRollOutQc, self).__init__(config)
        self.mysql_conf = self.get_param_config(['db', 'my-egn-qc'])
        env_name = self.get_param_config(['env_name'])
        db_key = {
            "prod": ["db", "pg-dp-services"],
            "stag": ["db", "pg-dp-stag"],
            "qc": ["db", "pg-dp-qc"]
        }.get(env_name)
        self.postgres_conf = self.get_param_config(db_key)
        self.from_date = self.get_process_info(['from_date']) + datetime.timedelta(hours=6)
        self.to_date = self.get_process_info(['to_date']) + datetime.timedelta(hours=6)

    def extract(self):
        sql = """
        select
            distinct email.channel_code,
            email.seeker_id,
            resume.id as resume_id
        from
            erp_seeker.seeker seeker
        join erp_seeker.resume resume
        on
            seeker.id = resume.seeker_id
            and resume.channel_code = seeker.channel_code
        join erp_seeker.seeker_email_marketing email 
                on
            email.channel_code = seeker.channel_code
            and email.seeker_id = seeker.id
        where
            resume.status = '1'
            and seeker.status = 1
            and email.status = 1
            and email.frequency in (1, 3, 7)
            -- and email.update_ts >= '{0}' and email.update_ts < '{1}' 
        """.format(self.from_date, self.to_date)

        df_tvn_seeker = load_df_from_mysql(self.mysql_conf, sql).drop_duplicates(
            ['channel_code', 'seeker_id'])
        df_tvn_seeker['model_type'] = 1

        df_email_seeker = df_tvn_seeker.drop_duplicates(
            ['seeker_id']
        )

        sql_cur_ab = """
        SELECT id, seeker_id, group_name
        FROM ja_roll_out"""
        df_cur_ab = load_df_from_postgres(self.postgres_conf, sql_cur_ab).drop_duplicates(
            ['seeker_id'])
        return df_email_seeker, df_cur_ab

    @AppBase.wrapper_simple_log
    def execute(self):
        self.log.info("# step 1: extract")
        df_email_seeker, df_cur_ab = self.extract()
        self.log.info("# step 2: transform")
        df_find_new, updated_df = self.transform(df_email_seeker, df_cur_ab)
        self.log.info("# step 3: transform")
        self.load(df_find_new, updated_df)

    def transform(self, df_email_seeker, df_cur_ab):
        # find new
        df_find_new = df_email_seeker.join(
            df_cur_ab.set_index(['seeker_id']),
            ['seeker_id'])
        ix_new = df_find_new.loc[df_find_new['group_name'].isna()].index
        df_find_new = df_find_new.loc[ix_new]
        if len(ix_new) > 0:
            df_find_new.loc[ix_new, 'group_name'] = 'B'
        else:
            self.log.warning('not found new data to group B')
        df_find_new = df_find_new.drop(columns=['id'])
        df_find_new['status'] = 1

        # find out
        updated_df = df_cur_ab.join(
            df_email_seeker.set_index(['seeker_id']),
            ['seeker_id'], how='inner')
        updated_df['status'] = 1

        return df_find_new, updated_df

    def load(self, df_find_new, updated_df):
        insert_df_to_postgres(
            self.postgres_conf, df=df_find_new.drop_duplicates(['seeker_id']),
            tbl_name="ja_roll_out",  primary_keys=['seeker_id']
        )
        insert_df_to_postgres(
            self.postgres_conf, df=updated_df.drop_duplicates(['seeker_id']),
            tbl_name="ja_roll_out",  primary_keys=['seeker_id']
        )
