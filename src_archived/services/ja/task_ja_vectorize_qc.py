import datetime
import math
import tqdm
from dateutil import tz
import os

from vncorenlp import VnCoreNLP
from gensim.models.keyedvectors import KeyedVectors
import tensorflow as tf
import numpy as np

from core.app_base import AppBase
from services.ja.preprocess_v2 import preprocess_job, preprocess_resume
from services.ja.vectorize_v2 import get_feature
from libs.storage_utils import load_df_from_mysql, insert_df_to_postgres, split_dataframe

local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class TaskJaVectorizeQc(AppBase):

    def __init__(self, config):
        super(TaskJaVectorizeQc, self).__init__(config)

        self.mysql_conf = self.get_param_config(['db', 'my-egn-qc'])
        rdrsegmenter_path = self.get_param_config(['rdrsegmenter_path'])
        self.rdrsegmenter = VnCoreNLP(
            rdrsegmenter_path, annotators="wseg", max_heap_size='-Xmx500m')
        # vncorenlp - Xmx500m < FULL - PATH - to - VnCoreNLP - jar - file > -p 9000 - a "wseg"
        # self.rdrsegmenter = VnCoreNLP(address="http://127.0.0.1", port=9000)

        env_name = self.get_param_config(['env_name'])
        db_key = {
            "prod": ["db", "pg-dp-services"],
            "stag": ["db", "pg-dp-stag"],
            "qc": ["db", "pg-dp-qc"]
        }.get(env_name)

        self.postgres_conf = self.get_param_config(db_key)

        self.from_date = self.get_process_info(['from_date']) + datetime.timedelta(hours=6) - \
            datetime.timedelta(days=2)
        self.to_date = self.get_process_info(['to_date']) + datetime.timedelta(hours=6)
        self.ml_version = self.get_param_config(['ml_version'])
        self.model_path = self.get_param_config(['model_path'])

        job_text_model_path = os.path.join(self.model_path, 'job_text_model.h5')
        job_title_model_path = os.path.join(self.model_path, 'job_title_model.h5')
        job_num_model = os.path.join(self.model_path, 'job_num_model.h5')
        res_text_model_path = os.path.join(self.model_path, 'res_text_model.h5')
        res_title_model_path = os.path.join(self.model_path, 'res_title_model.h5')
        res_num_model = os.path.join(self.model_path, 'res_num_model.h5')
        self.job_text_model = tf.keras.models.load_model(
            job_text_model_path, compile=False)
        self.job_title_model = tf.keras.models.load_model(
            job_title_model_path, compile=False)
        self.job_numerical_model = tf.keras.models.load_model(
            job_num_model, compile=False)
        self.res_text_model = tf.keras.models.load_model(
            res_text_model_path, compile=False)
        self.res_title_model = tf.keras.models.load_model(
            res_title_model_path, compile=False)
        self.res_numerical_model = tf.keras.models.load_model(
            res_num_model, compile=False)

        w2v_path = self.get_param_config(['w2v_path'])
        kv = KeyedVectors.load_word2vec_format(w2v_path, binary=False)
        init = tf.lookup.KeyValueTensorInitializer(
            keys=tf.constant(kv.index_to_key),
            values=tf.constant(np.arange(len(kv.index_to_key)), dtype=tf.int64))
        self.vocab_table = tf.lookup.StaticVocabularyTable(init, num_oov_buckets=1)

    def extract_res(self, from_date, to_date, query_col='update_ts'):
        res_sql = """
        select
          DISTINCT
          resume.channel_code, 
          resume.id as resume_id,
          resume.seeker_id,
          resume.title,
          resume.salary_range as salary,
          seeker.gender, 
          resume.level as degree,
          REPLACE(field_ids, ',', ',') as fields,
          REPLACE(province_ids, ',', ',') as provinces,
          resume.status,
          resume.is_search_allowed,
          resume.resume_type,
          resume.updated_at,
          resume.created_at,
          resume.update_ts
        from
          erp_seeker.resume resume
        left join erp_seeker.seeker seeker on resume.seeker_id = seeker.id
        where resume.channel_code='tvn'
          and resume.field_ids is not null
          and resume.status = 1
          -- and resume.{0} >= '{1}' and resume.{0} < '{2}'
        """.format(query_col, from_date, to_date)
        resume_df = load_df_from_mysql(self.mysql_conf, res_sql)
        return resume_df

    def extract_res_exp(self, from_date, to_date, query_col='update_ts'):
        res_exp_sql = """
        select
          distinct experience.channel_code,
          experience.id,
          experience.resume_id as resume_id,
          case
           when (experience.start_date is null
                 or experience.start_date < '1900-01-01') then '1970-01-01T00:00:00'
           when (experience.start_date > '2022-01-01') then current_timestamp
           else experience.start_date
          end start_date,
          experience.position,
          experience.company_name,
          experience.description
        from
          erp_seeker.resume resume
          join erp_seeker.resume_experience as experience 
          on experience.resume_id = resume.id and experience.channel_code = resume.channel_code
        where resume.channel_code='tvn'
          and resume.field_ids is not null
          and resume.status = 1
          -- and resume.{0} >= '{1}' and resume.{0} < '{2}'
        """.format(query_col, from_date, to_date)
        self.log.info(res_exp_sql)
        df_raw = load_df_from_mysql(self.mysql_conf, res_exp_sql)
        return df_raw

    def extract_job(self, from_date, to_date, query_col='update_ts'):
        job_sql = """
        select
          channel_code,
          id as job_id,
          employer_id, 
          title, 
          job_requirement as skill,
          description, 
          salary_range as salary,
          gender,
          degree_requirement as degree,
          CASE
            WHEN (field_ids_sub is null or field_ids_sub = '') THEN field_ids_main
            ELSE REPLACE(concat(field_ids_main, ',', field_ids_sub), ',', ',')
          END as fields,
          province_ids as provinces,
          status,
          created_at, 
          updated_at,
          update_ts
        from erp_employer.job job
        where channel_code='tvn'
          and job.status=1
          -- and job.{0} >= '{1}' and job.{0} < '{2}' 
        """.format(query_col, from_date, to_date)
        self.log.info(job_sql)
        df_raw = load_df_from_mysql(self.mysql_conf, job_sql)
        return df_raw

    def extract(self):
        df_job = self.extract_job(self.from_date, self.to_date).fillna('')
        df_res = self.extract_res(self.from_date, self.to_date).fillna('')
        df_res_exp = self.extract_res_exp(self.from_date, self.to_date).fillna('')
        df_res_exp['start_date'] = df_res_exp['start_date'].map(
            lambda x: datetime.datetime(1970, 1, 1, tzinfo=local_tz) if x == '' else x)
        log_info = """
        extract info
        job: {}
        res: {}
        res_exp: {}
        """.format(df_job.shape, df_res.shape, df_res_exp.shape)
        self.log.info(log_info)
        return df_job, df_res, df_res_exp

    @staticmethod
    def norm_str(s):
        rs = ''
        if isinstance(s, str):
            t = s.replace(' ', '').split(',')
            lst = list()
            for e in t:
                if e.isdigit():
                    lst.append(e)
            rs = ','.join(lst)
        return rs

    def execute_job(self):
        self.log.info("# step 1: extract job")
        df_job = self.extract_job(self.from_date, self.to_date).fillna('')
        log_info = """
        extract info
        job: {}
        """.format(df_job.shape)
        self.log.info(log_info)

        self.log.info("# step 2: tokenizer job")
        job_feature = self.tokenizer_job(df_job)
        job_feature['job_fields'] = job_feature['job_fields'].map(self.norm_str)
        job_feature['job_provinces'] = job_feature['job_provinces'].map(self.norm_str)

        with tqdm.tqdm(total=job_feature.shape[0]) as pbar:
            for _job_feature in split_dataframe(job_feature, 2000):
                self.log.info("# step 3.1: vectorize job")
                job_vec = self.vectorize_job(_job_feature)

                self.log.info("# step 3.2: load job")
                self.load_job(job_vec)
                pbar.update(job_vec.shape[0])

    def execute_resume(self):
        self.log.info("# step 1: extract resume")
        df_res = self.extract_res(self.from_date, self.to_date).fillna('')
        df_res_exp = self.extract_res_exp(self.from_date, self.to_date).fillna('')

        log_info = """
        extract info
        res: {}
        res_exp: {}
        """.format(df_res.shape, df_res_exp.shape)
        self.log.info(log_info)

        df_res_exp['start_date'] = df_res_exp['start_date'].map(
            lambda x: datetime.datetime(1970, 1, 1, tzinfo=local_tz) if x == '' else x)

        self.log.info("# step 2: tokenizer resume")
        resume_feature = self.tokenizer_resume(df_res, df_res_exp)

        resume_feature['resume_fields'] = resume_feature['resume_fields'].map(self.norm_str)
        resume_feature['resume_provinces'] = resume_feature['resume_provinces'].map(self.norm_str)

        with tqdm.tqdm(total=resume_feature.shape[0]) as pbar:
            for _resume_feature in split_dataframe(resume_feature, 2000):
                self.log.info("# step 4.1: vectorize resume")
                resume_vec = self.vectorize_resume(_resume_feature)

                self.log.info("# step 4.2: load resume")
                self.load_resume(resume_vec)
                pbar.update(resume_vec.shape[0])

    @AppBase.wrapper_simple_log
    def execute(self):
        self.log.info("# step: execute job")
        self.execute_job()
        # df_job, df_res, df_res_exp = self.extract()

        self.log.info("# step: execute resume")
        self.execute_resume()
        # job_feature, resume_feature = self.tokenizer(df_job, df_res, df_res_exp)

        # job_feature['job_fields'] = job_feature['job_fields'].str.replace(
        #     r",$", '').str.replace(' ', '').str.replace(',,', ',')
        # job_feature['job_provinces'] = job_feature['job_provinces'].str.replace(
        #     r",$", '').str.replace(' ', '').str.replace(',,', ',')
        # resume_feature['resume_fields'] = resume_feature['resume_fields'].str.replace(
        #     r",$", '').str.replace(' ', '').str.replace(',,', ',')
        # resume_feature['resume_provinces'] = resume_feature['resume_provinces'].str.replace(
        #     r",$", '').str.replace(' ', '').replace(',,', ',')
        self.rdrsegmenter.close()

        # with tqdm.tqdm(total=job_feature.shape[0]) as pbar:
        #     for _job_feature in split_dataframe(job_feature, 2000):
        #         self.log.info("# step 3.1: vectorize job")
        #         job_vec = self.vectorize_job(_job_feature)
        #
        #         self.log.info("# step 3.2: load job")
        #         self.load_job(job_vec)
        #         pbar.update(job_vec.shape[0])

        # with tqdm.tqdm(total=resume_feature.shape[0]) as pbar:
        #     for _resume_feature in split_dataframe(resume_feature, 2000):
        #         self.log.info("# step 4.1: vectorize resume")
        #         resume_vec = self.vectorize_resume(_resume_feature)
        #
        #         self.log.info("# step 4.2: load resume")
        #         self.load_resume(resume_vec)
        #         pbar.update(resume_vec.shape[0])

        # self.log.info("# step 3: vectorize")
        # job_vec, resume_vec = self.vectorize(job_feature, resume_feature)
        #
        # self.log.info("# step 4: load")
        # self.load(job_vec, resume_vec)

    def tokenizer_job(self, df_job):
        if df_job.shape[0] > 0:
            job_feature = preprocess_job(df_job, self.rdrsegmenter).fillna('').join(
                df_job[['channel_code', 'employer_id', 'status', 'created_at', 'updated_at', 'update_ts']])
            job_feature['status'] = job_feature['status'].map(str).map(lambda x: x.replace('.0', ''))
        else:
            job_feature = None
        return job_feature

    def tokenizer_resume(self, df_res, df_res_exp):
        if df_res.shape[0] > 0:
            resume_feature = preprocess_resume(df_res, df_res_exp, self.rdrsegmenter).fillna('').join(
                df_res[['channel_code', 'seeker_id', 'resume_id', 'created_at', 'updated_at', 'update_ts',
                        'status', 'is_search_allowed']].set_index(['seeker_id', 'resume_id']),
                ['seeker_id', 'resume_id']
            )
            resume_feature['status'] = resume_feature[
                'status'].map(str).map(lambda x: x.replace('.0', ''))
            resume_feature['is_search_allowed'] = resume_feature[
                'is_search_allowed'].map(str).map(lambda x: x.replace('.0', ''))
            resume_feature['update_ts'] = resume_feature.apply(
                lambda x: x['updated_at'] if isinstance(x['update_ts'], str) else x['update_ts'], axis=1)
        else:
            resume_feature = None
        return resume_feature

    def tokenizer(self, df_job, df_res, df_res_exp):
        job_feature = self.tokenizer_job(df_job)
        resume_feature = self.tokenizer_resume(df_res, df_res_exp)
        return job_feature, resume_feature

    def vectorize_job(self, job_feature):
        tmp_job_title = job_feature[['job_id', 'job_title']]
        tmp_job_title.columns = ['job_id', 'text']
        job_title_vec = get_feature(
            tmp_job_title, model=self.job_title_model,
            vocab_table=self.vocab_table, mode='title').tolist()

        tmp_job_text = job_feature[['job_id', 'requirement']]
        tmp_job_text.columns = ['job_id', 'text']
        job_text_vec = get_feature(
            tmp_job_text, model=self.job_text_model,
            vocab_table=self.vocab_table, mode='text').tolist()

        tmp_job_category = job_feature[['job_id', 'job_salary', 'job_degree', 'job_gender', 'job_fields']]
        tmp_job_category.columns = ['job_id', 'salary', 'degree', 'gender', 'fields']

        job_cat_vec = get_feature(
            tmp_job_category, model=self.job_numerical_model,
            vocab_table=self.vocab_table, mode='cat').tolist()

        job_vec = job_feature[['channel_code', 'employer_id', 'job_id']]
        job_vec['job_text'] = job_text_vec
        job_vec['job_title'] = job_title_vec
        job_vec['job_category'] = job_cat_vec
        return job_vec

    def vectorize_resume(self, resume_feature):
        # Get resume text feature
        tmp_res_text = resume_feature[['resume_id', 'text']]
        # #Sort the resume experience by date order
        # tmp_res_text = resume_feature[['resume_id', 'text',
        #                                'start_date']].sort_values(by='start_date',
        #                                                           ascending=False)
        # tmp_res_text = tmp_res_text[['resume_id', 'text']]
        tmp_res_text.columns = ['resume_id', 'text']
        res_text_vec = get_feature(
            tmp_res_text, model=self.res_text_model,
            vocab_table=self.vocab_table, mode='text').tolist()

        # Get resume title feature
        tmp_res_title = resume_feature[['resume_id', 'resume_title']]
        tmp_res_title.columns = ['resume_id', 'text']
        res_title_vec = get_feature(
            tmp_res_title, model=self.res_title_model,
            vocab_table=self.vocab_table, mode='title').tolist()

        # Get resume categorical feature
        tmp_res_category = resume_feature[[
            'resume_id', 'resume_salary', 'resume_degree', 'resume_gender', 'resume_fields']]
        tmp_res_category.columns = ['resume_id', 'salary', 'degree', 'gender', 'fields']
        res_cat_vec = get_feature(tmp_res_category, model=self.res_numerical_model,
                                  vocab_table=self.vocab_table, mode='cat').tolist()

        res_vec = resume_feature[['channel_code', 'seeker_id', 'resume_id']]
        res_vec['resume_text'] = res_text_vec
        res_vec['resume_title'] = res_title_vec
        res_vec['resume_category'] = res_cat_vec
        return res_vec

    def vectorize(self, job_feature, resume_feature):
        job_vec = self.vectorize_job(job_feature)
        resume_vec = self.vectorize_resume(resume_feature)
        return job_vec, resume_vec

    def load_job(self, job_vec):
        log_info = """
        load info
        job_vec : {}
        """.format(job_vec.shape)
        self.log.info(log_info)
        insert_df_to_postgres(
            self.postgres_conf, df=job_vec,
            tbl_name="me_job_vec_{}".format(self.ml_version),
            primary_keys=['job_id'])

    def load_resume(self, resume_vec):
        log_info = """
        load info
        resume_vec: {}
        """.format(resume_vec.shape)
        self.log.info(log_info)
        insert_df_to_postgres(
            self.postgres_conf, df=resume_vec,
            tbl_name="me_resume_vec_{}".format(self.ml_version),
            primary_keys=['resume_id'])

    def load(self, job_vec, resume_vec):
        log_info = """
        load info
        job_vec : {}
        resume_vec: {}
        """.format(job_vec.shape, resume_vec.shape)
        self.log.info(log_info)
        insert_df_to_postgres(
            self.postgres_conf, df=job_vec,
            tbl_name="me_job_vec_{}".format(self.ml_version), primary_keys=['job_id'])
        insert_df_to_postgres(
            self.postgres_conf, df=resume_vec,
            tbl_name="me_resume_vec_{}".format(self.ml_version), primary_keys=['resume_id'])

    def backfill_days(self, from_date: datetime.datetime, to_date: datetime, interval_days=30, query_col='created_at'):
        delta_days = math.ceil((to_date - from_date) / datetime.timedelta(days=interval_days))
        with tqdm.tqdm(total=delta_days) as pbar:
            for i in range(delta_days):
                _from_date = from_date + i * datetime.timedelta(days=interval_days)
                _to_date = from_date + (i + 1) * datetime.timedelta(days=interval_days)
                log_info = """ {} - {} """.format(_from_date.isoformat(), _to_date.isoformat())
                self.log.info(log_info)
                if _to_date > to_date:
                    _to_date = to_date
                df_res = self.extract_res(_from_date, _to_date, query_col).fillna('')
                df_res_exp = self.extract_res_exp(_from_date, _to_date, query_col).fillna('')
                df_res_exp['start_date'] = df_res_exp['start_date'].map(
                    lambda x: datetime.datetime(1970, 1, 1, tzinfo=local_tz) if x == '' else x)
                self.log.info("# step: tokenizer")
                resume_feature = self.tokenizer_resume(df_res, df_res_exp)
                resume_feature['resume_fields'] = resume_feature['resume_fields'].str.replace(
                    r",$", '').str.replace(' ', '').str.replace(',,', ',')
                resume_feature['resume_provinces'] = resume_feature['resume_provinces'].str.replace(
                    r",$", '').str.replace(' ', '').replace(',,', ',')

                with tqdm.tqdm(total=resume_feature.shape[0]) as _pbar:
                    for _resume_feature in split_dataframe(resume_feature, 2000):
                        self.log.info("# step: vectorize resume")
                        resume_vec = self.vectorize_resume(_resume_feature)

                        self.log.info("# step: load resume")
                        self.load_resume(resume_vec)
                        _pbar.update(resume_vec.shape[0])
                pbar.update(1)
