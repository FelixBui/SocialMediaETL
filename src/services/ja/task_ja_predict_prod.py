import datetime
import gc
import itertools
import pandas as pd
import tensorflow as tf
import tqdm
from dateutil import tz

from core.app_base import AppBase
from services.ja.data_gen_v2 import DataGenerator
from libs.storage_utils import load_df_from_postgres, upload_df_to_gcs, get_gcs_cli, \
    get_rmq_channel, get_rmq_cli, push_rmq_channel, load_df_from_bq, load_sa_creds


local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


def split_dataframe(df, chunk_size=5000):
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        yield df[i * chunk_size: (i + 1) * chunk_size]


def grouper_it(it, chuck_size):
    while True:
        chunk_it = itertools.islice(it, chuck_size)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)


class TaskJaPredictProd(AppBase):

    def __init__(self, config):
        super(TaskJaPredictProd, self).__init__(config)
        env_name = self.get_param_config(['env_name'])
        db_key = {
            "prod": ["db", "pg-dp-services"],
            "stag": ["db", "pg-dp-stag"],
            "qc": ["db", "pg-dp-qc"]
        }.get(env_name)
        self.postgres_conf = self.get_param_config(db_key)
        dp_ops = self.get_param_config(['dp-ops'])
        self.bucket_name = self.get_param_config(["bucket_name"])
        self.bq_creds = load_sa_creds(dp_ops)
        self.gcs_cli = get_gcs_cli(dp_ops)
        self.from_date = self.get_process_info(['from_date']) + datetime.timedelta(hours=6)
        self.to_date = self.get_process_info(['to_date']) + datetime.timedelta(hours=6)
        self.ml_version = self.get_param_config(['ml_version'])
        self.instance_id = 0  # int(self.get_process_info(['instance_id']))
        # self.total_instance = int(self.get_process_info(['total_instances']))
        # config_dir = os.environ['CONFIG_DIR']
        network_train_model_path = self.get_param_config(['network_train_model_path'])
        self.network_train = tf.keras.models.load_model(
            network_train_model_path, compile=False
        )
        self.load_size = self.get_param_config(['load_size'])
        self.chuck_size = self.get_param_config(['chuck_size'])
        self.batch_size = self.get_param_config(['batch_size'])
        self.rmq_conf = self.get_param_config(['db', 'rb-rabbitmq'])
        self.rmq_channel_name = self.get_param_config(['rmq_channel_name'])

    def extract(self):
        job_sql = """
        SELECT job.channel_code,
               job.id as job_id,
               job.salary_range as job_salary,
               CASE
                   WHEN (field_ids_sub is null
                         or field_ids_sub = '') THEN field_ids_main
                   ELSE REPLACE(concat(field_ids_main, ',', field_ids_sub), ',', ',')
               END as job_fields,
               job.province_ids as job_provinces
        FROM src_raw.adv2_emp_job job
        JOIN src_adv2_so.running_job_box run on job.channel_code=run.channel_code and job.id = run.job_id
        WHERE run.channel_code='tvn'
        """
        job_vec_sql = """
        SELECT vec.* 
        FROM me_job_vec_{0} vec
        WHERE vec.job_id in ({1})
        """

        resume_sql = """
        SELECT 
            resume.channel_code,
            resume.id as resume_id,
            resume.seeker_id,
            resume.salary_range as resume_salary,
            REPLACE(field_ids, ',', ',') as resume_fields,
            REPLACE(province_ids, ',', ',') as resume_provinces
        FROM src_raw.adv2_seeker_resume resume
        JOIN matching_score.test_ja_roll_out ab ON resume.id = cast(ab.resume_id as int) 
        WHERE cast(ab.status as int) = 1
        """
        resume_vec_sql = """
        SELECT vec.* 
        FROM me_resume_vec_{0} vec
        WHERE vec.resume_id in ({1})
        """
        # job_sql = self.job_sql.format(self.to_date)
        # res_sql = self.res_sql.format(self.to_date)
        # delta_time = (self.to_date - self.from_date) / self.total_instance
        # from_date = self.from_date + delta_time * (self.instance_id - 1)
        # to_date = from_date + delta_time
        # self.log.info("time update: {} - {}".format(from_date, to_date))
        df_job_active = load_df_from_bq(self.bq_creds, job_sql)
        df_res_active = load_df_from_bq(self.bq_creds, resume_sql)

        df_job_vec = load_df_from_postgres(
            self.postgres_conf, job_vec_sql.format(
                self.ml_version, ", ".join(df_job_active['job_id'].map(str))
            )
        )
        df_res_vec = load_df_from_postgres(
            self.postgres_conf, resume_vec_sql.format(
                self.ml_version, ", ".join(df_res_active['resume_id'].map(str))
            )
        )
        log_info = """
        extract info
        job active: {}
        res active: {}
        job vec: {}
        res vec: {}
        """.format(df_job_active.shape, df_res_active.shape, df_job_vec.shape, df_res_vec.shape)
        self.log.info(log_info)

        # n_sub = math.ceil(df_res_active.shape[0] / self.total_instance)
        # ix = self.instance_id - 1
        # df_res_active = df_res_active.iloc[ix*n_sub:(ix+1)*n_sub, :]
        # df_job_update = df_job_active.loc[
        #     (df_job_active['updated_at'] >= self.from_date)
        #     & (df_job_active['updated_at'] < self.to_date)
        # ]
        # df_res_update = df_res_active.loc[
        #     (df_res_active['updated_at'] >= from_date)
        #     & (df_res_active['updated_at'] < to_date)
        # ]
        # df_res_update['dump_json'] = df_res_update[[
        #     'resume_title', 'text', 'resume_salary',
        #     'resume_degree', 'resume_gender', 'resume_fields',
        #     'resume_provinces']].apply(lambda x: x.to_json(), axis=1)
        # df_res_update['sha256'] = df_res_update['dump_json'].map(lambda x: sha256(x.encode('utf-8')).hexdigest())
        #
        # df_job_update['dump_json'] = df_job_update[[
        #     'job_title', 'requirement', 'job_salary',
        #     'job_degree', 'job_gender', 'job_fields', 'job_provinces']].apply(
        #             lambda x: x.to_json(), axis=1)
        # df_job_update['sha256'] = df_job_update['dump_json'].map(lambda x: sha256(x.encode('utf-8')).hexdigest())
        #
        # df_resume_hash = load_df_from_postgres(
        #     self.postgres_conf, "select resume_id, sha256, last_check from resume_sha256")
        # df_job_hash = load_df_from_postgres(
        #     self.postgres_conf, "select job_id, sha256, last_check from job_sha256")
        # t0 = df_job_update.join(df_job_hash.set_index(['job_id', 'sha256']), ['job_id', 'sha256'])
        # df_job_update = t0[t0['last_check'].isna()]
        # t1 = df_res_update.join(df_resume_hash.set_index(['resume_id', 'sha256']), ['resume_id', 'sha256'])
        # df_res_update = t1[t1['last_check'].isna()]
        # df_res_update = df_res_active.iloc[0:0, :].copy()
        # df_job_update = df_job_active.iloc[0:0, :].copy()
        # log_info = """
        # extract info
        # job active: {}
        # res active: {}
        # """.format(df_job_active.shape, df_res_active.shape)
        # self.log.info(log_info)
        return df_job_active, df_res_active, df_job_vec, df_res_vec

    @AppBase.wrapper_simple_log
    def execute(self):
        self.log.info("# step 1: extract")
        df_job_active, df_res_active, df_job_vec, df_res_vec = self.extract()

        self.log.info("# step 2: transform")
        it_loop = self.transform(df_job_active, df_res_active)

        self.log.info("# step 3: predict")
        # estimate = df_job_active.shape[0] * df_res_update.shape[0] + df_job_update.shape[0] * df_res_active.shape[0]
        estimate = df_job_active.shape[0] * df_res_active.shape[0]
        loop = 0
        with tqdm.tqdm(total=estimate) as pbar:
            for df_app in it_loop:
                estimate_batch = df_app.shape[0]
                df_app = df_app.drop_duplicates(['job_id', 'resume_id'])
                df_point = self.predict(df_app, df_job_active, df_res_active, df_job_vec, df_res_vec)
                if df_point.shape[0] > 0:
                    df_point['model_version'] = self.ml_version
                    df_point['updated_at'] = datetime.datetime.now().replace(tzinfo=local_tz)
                    self.log.info("# step 4: load")
                    self.load(df_point, loop)
                loop += 1
                self.log.info("gc collect {}".format(gc.collect()))
                pbar.update(estimate_batch)

    def transform(self, df_job_active, df_res_active):
        self.log.info(
            "job active: {} | res active: {} | app estimate {}".format(
                df_job_active.shape[0],
                df_res_active.shape[0],
                df_job_active.shape[0] * df_res_active.shape[0]
            )
        )
        # it = itertools.chain(
        #     itertools.product(df_job_active['job_id'], df_res_update['resume_id']),
        #     itertools.product(df_job_update['job_id'], df_res_active['resume_id'])
        # )
        it = itertools.product(df_job_active['job_id'], df_res_active['resume_id'])
        for ir in grouper_it(it, self.load_size):
            yield pd.DataFrame(list(ir), columns=['job_id', 'resume_id'])

    def load(self, point_df, loop):
        self.log.info("shape: {0} | loop: {1}".format(point_df.shape, loop))
        self.log.info("sample {0}".format(point_df.iloc[0]))
        in_fmt = "dp-services/ja/{}/{}/{}.pq"
        obj_key = in_fmt.format(
            str(self.execution_date.astimezone(local_tz).date()),
            self.instance_id, str(loop).zfill(3)
        )

        upload_df_to_gcs(
            self.gcs_cli, point_df, bucket_name=self.bucket_name, gcs_path=obj_key)
        msg = {
            "bucket_name": self.bucket_name,
            "object_key": obj_key,
        }
        rmq_cli = get_rmq_cli(self.rmq_conf)
        rmq_channel = get_rmq_channel(rmq_cli, self.rmq_channel_name)
        push_rmq_channel(rmq_channel, self.rmq_channel_name, msg)
        rmq_channel.close()
        rmq_cli.close()

    # insert_df_to_postgres(
    #     self.postgres_conf, df=df_point,
    #     tbl_name="score_full",
    #     primary_keys=["resume_id", "job_id"],
    #     schema="matching_score")

    @staticmethod
    def is_match_province_fields(x, y):
        for e1 in x.split(','):
            for e2 in y.split(','):
                if e1 == e2:
                    return True
        return False

    def predict(self, df_app, df_job_active, df_res_active,
                df_job_vec, df_res_vec):
        lst_result = list()
        self.log.info("chunk size {}, batch_size {}".format(self.chuck_size, self.batch_size))
        with tqdm.tqdm(total=df_app.shape[0]) as pbar:
            for df_in_app in split_dataframe(df_app, self.chuck_size):
                if df_in_app.shape[0] == 0:
                    continue
                df_in = df_in_app.merge(
                    df_job_active[['job_id', 'job_salary', 'job_fields', 'job_provinces']],
                    on='job_id',
                ).merge(
                    df_res_active[['seeker_id', 'resume_id', 'resume_salary', 'resume_fields', 'resume_provinces']],
                    on='resume_id',
                )
                df_in['is_match_provinces'] = df_in[['job_provinces', 'resume_provinces']].fillna('').apply(
                    lambda x: self.is_match_province_fields(x['job_provinces'], x['resume_provinces']), axis=1)
                df_in['is_match_fields'] = df_in[['job_fields', 'resume_fields']].fillna('').apply(
                    lambda x: self.is_match_province_fields(x['job_fields'], x['resume_fields']), axis=1)
                df_in['is_match_salary'] = df_in['job_salary'] >= df_in['resume_salary']
                df_selected = df_in.loc[df_in['is_match_provinces']
                                        & df_in['is_match_fields']
                                        & df_in['is_match_salary'],
                                        ['seeker_id', 'job_id', 'resume_id']].reset_index(drop=True)

                if df_selected.shape[0] > 0:
                    df_feature = df_selected.merge(
                        df_job_vec[['job_id', 'job_text', 'job_title', 'job_category']],
                        on='job_id',
                    ).merge(
                        df_res_vec[['resume_id', 'resume_text', 'resume_title', 'resume_category']],
                        on='resume_id',
                    )
                    if df_feature.shape[0] > 0:
                        df_out = df_feature[['seeker_id', 'job_id', 'resume_id']]
                        generator = DataGenerator(
                            data=df_feature[['job_text', 'job_title', 'job_category',
                                             'resume_text', 'resume_title', 'resume_category']].values,
                            batch_size=128)
                        predict_history = self.network_train.predict(generator)
                        df_out['score'] = predict_history.squeeze().tolist()
                        lst_result.append(df_out)
                pbar.update(df_in.shape[0])
        if len(lst_result):
            return pd.concat(lst_result)
        else:
            return pd.DataFrame()
