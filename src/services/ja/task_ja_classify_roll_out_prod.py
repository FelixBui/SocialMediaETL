from dateutil import tz
import datetime

from core.app_base import AppBase
from libs.storage_utils import load_sa_creds, insert_df_to_postgres, load_df_from_postgres, \
    load_df_from_bq, insert_df_to_bq

local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class TaskJaClassifyRollOutProd(AppBase):

    def __init__(self, config):
        super(TaskJaClassifyRollOutProd, self).__init__(config)
        self.dp_ops = self.get_param_config(['dp-ops'])
        self.bq_creds = load_sa_creds(self.dp_ops)
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
        sql = """with resume_new as (
        select * from src_raw.adv2_seeker_resume resume
        where STRUCT(channel_code, seeker_id, id) in (
            select as STRUCT channel_code, seeker_id, max(id)
            from dwh_prod.src_adv2_seeker_resume resume
            where channel_code='tvn' and status=1 and resume_type=1
            group by 1, 2)
        )
        select
          distinct email.channel_code, email.seeker_id, email.resume_id
        from
          src_raw.adv2_seeker_seeker_email_marketing email
          join resume_new resume 
             on email.resume_id = resume.id and email.channel_code = resume.channel_code 
             and email.seeker_id = resume.seeker_id
          join src_raw.adv2_seeker_resume_experience experience 
             on email.seeker_id = experience.seeker_id and email.channel_code = experience.channel_code 
             and email.resume_id = experience.resume_id
          join src_raw.adv2_seeker_seeker seeker 
             on email.seeker_id = seeker.id and email.channel_code = seeker.channel_code
          join dwh_prod.src_adv2_tracking_mail_email_open email_open 
             on email.channel_code = email_open.channel_code and email.seeker_id = email_open.object_id 
             and email_open.object_type = 'seeker'
        where
          email.channel_code = 'tvn'
          and email.status = 1
          and email.seeker_id is not null
          and resume.status = 1
          and seeker.status = 1
          and frequency in (1, 3, 7)
          and resume.resume_type = 1
          and resume.id is not null
          and (
            resume.update_ts >= datetime_sub(TIMESTAMP '{0}', INTERVAL 150 DAY)
            AND resume.update_ts < '{0}'
          )
          and email_open.created_at > datetime_sub(TIMESTAMP '{0}', INTERVAL 60 DAY)
        """.format(self.to_date)

        df_tvn_seeker = load_df_from_bq(self.bq_creds, sql).drop_duplicates(
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
        df = load_df_from_postgres(self.postgres_conf, "select * from ja_roll_out")
        insert_df_to_bq(
            bq_conf=self.dp_ops,
            tbl_name="matching_score.test_ja_roll_out",
            df=df.drop_duplicates(['seeker_id']),
            mode="replace"
        )
