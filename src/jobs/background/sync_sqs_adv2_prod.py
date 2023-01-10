import datetime
import json
import boto3
import pandas as pd
from dateutil import tz
import time
import requests

from core.app_base import AppBase
from libs.storage_utils import insert_df_to_postgres

local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class SyncSqsAdv2Prod(AppBase):
    def __init__(self, config):
        super().__init__(config)
        aws_certs = self.get_param_config(['aws_certs'])
        self.boto3_session = boto3.Session(
            aws_access_key_id=aws_certs['aws_access_key_id'],
            aws_secret_access_key=aws_certs['aws_secret_access_key'],
            region_name=aws_certs["region_name"]
        )
        self.sqs = self.boto3_session.resource('sqs')
        self.queue_urls = self.get_param_config(['queue_urls'])
        self.fail_queue_url = self.get_param_config(['fail_queue_url'])
        self.postgres_conf = self.get_param_config(['db', 'db_data_services'])
        self.interval_check = 600  # 10 minutes
        self.interval_auth_check = 60 * 60 * 12  # 12 hours
        self._next_check = time.time() + self.interval_check
        self._next_auth = time.time() + self.interval_auth_check
        self.auth_api_conf = self.get_param_config(["auth_rs_api"])
        self.auth_conf = self.auth_api_conf['auth']
        self.headers = None
        self.get_token()

    def get_token(self):
        res = requests.post(
            self.auth_conf['endpoint'],
            headers={
                "accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded"
            },
            data={
                "username": self.auth_conf['username'],
                "password": self.auth_conf["password"]
            }
        )
        token = res.json()
        self.headers = {
            "Authorization": "{} {}".format(token['token_type'], token['access_token'])
        }

    @staticmethod
    def parse_resume(x):
        x['created_at'] = datetime.datetime.fromtimestamp(x['created_at']).replace(tzinfo=local_tz)
        x['updated_at'] = datetime.datetime.fromtimestamp(x['updated_at']).replace(tzinfo=local_tz) \
            if x['updated_at'] else x['updated_at']
        x['update_ts'] = datetime.datetime.fromtimestamp(x['update_ts']).replace(tzinfo=local_tz) \
            if x['update_ts'] else x['update_ts']
        return x

    @staticmethod
    def parse_job(x):
        x['created_at'] = datetime.datetime.fromtimestamp(x['created_at']).replace(tzinfo=local_tz)
        x['updated_at'] = datetime.datetime.fromtimestamp(x['updated_at']).replace(tzinfo=local_tz) \
            if x['updated_at'] else x['updated_at']
        x['update_ts'] = datetime.datetime.fromtimestamp(x['update_ts']).replace(tzinfo=local_tz) \
            if x['update_ts'] else x['update_ts']
        return x

    @staticmethod
    def parse_seeker(x):
        x['created_at'] = datetime.datetime.fromtimestamp(x['created_at']).replace(tzinfo=local_tz)
        x['updated_at'] = datetime.datetime.fromtimestamp(x['updated_at']).replace(tzinfo=local_tz) \
            if x['updated_at'] else x['updated_at']
        x['update_ts'] = datetime.datetime.fromtimestamp(x['update_ts']).replace(tzinfo=local_tz) \
            if x['update_ts'] else x['update_ts']
        x['logined_at'] = datetime.datetime.fromtimestamp(x['logined_at']).replace(tzinfo=local_tz) \
            if x['logined_at'] else x['logined_at']
        return x

    @staticmethod
    def parse_employer(x):
        x['created_at'] = datetime.datetime.fromtimestamp(x['created_at']).replace(tzinfo=local_tz)
        x['updated_at'] = datetime.datetime.fromtimestamp(x['updated_at']).replace(tzinfo=local_tz) \
            if x['updated_at'] else x['updated_at']
        x['update_ts'] = datetime.datetime.fromtimestamp(x['update_ts']).replace(tzinfo=local_tz) \
            if x['update_ts'] else x['update_ts']
        return x

    @staticmethod
    def parse_employer_freemium(x):
        x['created_at'] = datetime.datetime.fromtimestamp(x['created_at']).replace(tzinfo=local_tz)
        x['updated_at'] = datetime.datetime.fromtimestamp(x['updated_at']).replace(tzinfo=local_tz) \
            if x['updated_at'] else x['updated_at']
        return x

    @staticmethod
    def parse_registration_job_box(x):
        x['created_at'] = datetime.datetime.fromtimestamp(x['created_at']).replace(tzinfo=local_tz)
        x['updated_at'] = datetime.datetime.fromtimestamp(x['updated_at']).replace(tzinfo=local_tz) \
            if x['updated_at'] else x['updated_at']
        x['update_ts'] = datetime.datetime.fromtimestamp(x['update_ts']).replace(tzinfo=local_tz) \
            if x['update_ts'] else x['update_ts']
        x['expired_at'] = datetime.datetime.fromtimestamp(x['expired_at']).replace(tzinfo=local_tz) \
            if x['expired_at'] else x['expired_at']
        x['start_date'] = datetime.datetime.fromtimestamp(x['start_date']).replace(tzinfo=local_tz) \
            if x['start_date'] else x['start_date']
        x['end_date'] = datetime.datetime.fromtimestamp(x['end_date']).replace(tzinfo=local_tz) \
            if x['end_date'] else x['end_date']
        return x

    @staticmethod
    def parse_running_job_box(x):
        x['created_at'] = datetime.datetime.fromtimestamp(x['created_at']).replace(tzinfo=local_tz)
        x['updated_at'] = datetime.datetime.fromtimestamp(x['updated_at']).replace(tzinfo=local_tz) \
            if x['updated_at'] else x['updated_at']
        x['update_ts'] = datetime.datetime.fromtimestamp(x['update_ts']).replace(tzinfo=local_tz) \
            if x['update_ts'] else x['update_ts']
        x['expired_at'] = datetime.datetime.fromtimestamp(x['expired_at']).replace(tzinfo=local_tz) \
            if x['expired_at'] else x['expired_at']
        return x

    def send_msg_fail(self, msg):
        fail_queue = self.sqs.Queue(self.fail_queue_url)
        response = fail_queue.send_message(
            QueueUrl=self.fail_queue_url,
            DelaySeconds=0,
            MessageAttributes={
                'Author': {
                    'DataType': 'String',
                    'StringValue': 'Thuc Phan'
                },
                "Project": {
                    'DataType': 'String',
                    'StringValue': 'SQS'
                },
                "Env": {
                    'DataType': 'String',
                    'StringValue': 'Prod'
                }
            },
            MessageBody=msg
        )
        self.log.debug(response['MessageId'])

    @staticmethod
    def drop_cols(df, cols):
        df_cols = df.columns
        for col in df_cols:
            if col not in cols:
                df = df.drop(columns=[col])
        return df

    @AppBase.wrapper_simple_log
    def execute(self):
        queues = list()
        processed = list()
        for queue_url in self.queue_urls:
            queue = self.sqs.Queue(queue_url)
            queues.append(queue)

        while True:
            for queue in queues:
                rs = dict()
                for k in ["resume", "job", "seeker", "employer",
                          "employer_freemium", "registration_job_box",
                          "running_job_box"]:
                    rs[k] = list()

                response = queue.receive_messages(
                    AttributeNames=[
                        'SentTimestamp'
                    ],
                    MaxNumberOfMessages=10,
                    MessageAttributeNames=[
                        'All'
                    ],
                    VisibilityTimeout=15,
                    WaitTimeSeconds=10
                )
                for e in response:
                    try:
                        msg = json.loads(e.body)
                        url = e.queue_url
                        self.log.debug(e.body)
                        k = {
                            "https://sqs.ap-southeast-1.amazonaws.com/985597616120/sqs-rs-job": 'job',
                            "https://sqs.ap-southeast-1.amazonaws.com/985597616120/sqs-rs-resume": 'resume',
                            "https://sqs.ap-southeast-1.amazonaws.com/985597616120/sqs-rs-seeker": 'seeker',
                            "https://sqs.ap-southeast-1.amazonaws.com/985597616120/sv-sqs-employer": 'employer',
                            "https://sqs.ap-southeast-1.amazonaws.com/985597616120/sv-sqs-employer-freemium":
                                'employer_freemium'
                        }.get(url)
                        fm = {
                            "job": self.parse_job,
                            "resume": self.parse_resume,
                            "seeker": self.parse_seeker,
                            "employer": self.parse_employer,
                            "employer_freemium": self.parse_employer_freemium,
                            "registration_job_box": self.parse_registration_job_box,
                            "running_job_box": self.parse_running_job_box
                        }
                        rs[k].append(fm[k](msg))
                        processed.append((msg['id'], k))

                    except Exception as ex:
                        err = """{} - {} - {}""".format('SQS', 'Prod', str(ex))
                        self.telegram_bot.send_message(self.owner_id, err)
                        self.send_msg_fail(e.body)
                        # raise Exception
                    finally:
                        e.delete()

                for k, v in rs.items():
                    if len(v):
                        df = pd.DataFrame(v)
                        if k == "job":
                            cols = [
                                "channel_code", "id", "employer_id", "title", "job_requirement",
                                "description", "salary_range", "gender", "degree_requirement",
                                "field_ids_main", "field_ids_sub", "province_ids", "status",
                                "premium_type", "total_resume_applied", "is_forbid", "created_at",
                                "updated_at", "update_ts", "vacancy_quantity", "age_range",
                                "experience_range", "benefit", "level_requirement"]
                            df = self.drop_cols(df, cols)
                            insert_df_to_postgres(
                                self.postgres_conf, tbl_name="adv2_job",
                                df=df, primary_keys=['id'])
                            requests.post(
                                self.auth_api_conf["job_endpoint"],
                                headers=self.headers,
                                json={"jobs": df['id'].tolist()}
                            )
                        elif k == "resume":
                            cols = [
                                "channel_code", "id", "seeker_id", "title", "salary_range", "gender",
                                "level", "position", "current_position", "field_ids", "province_ids",
                                "experience", "status", "is_search_allowed", "resume_type", "updated_at",
                                "created_at", "update_ts", "career_objective", "work_time", "birthday", "cv_file"]
                            df = self.drop_cols(df, cols)
                            insert_df_to_postgres(
                                self.postgres_conf, tbl_name="adv2_resume",
                                df=df, primary_keys=['id']
                            )
                            requests.post(
                                self.auth_api_conf["res_endpoint"],
                                headers=self.headers,
                                json={"resumes": df['id'].tolist()}
                            )

                        elif k == "seeker":
                            cols = [
                                "channel_code", "id", "name", "gender", "status", "logined_at",
                                "created_at", "updated_at", "update_ts", "province_id"]
                            df = self.drop_cols(df, cols)
                            insert_df_to_postgres(
                                self.postgres_conf, tbl_name="adv2_seeker",
                                df=df, primary_keys=['id']
                            )
                        elif k == "employer":
                            cols = [
                                "channel_code", "id", "branch_code", "name",
                                "description", "address", "province_id", "email_verified_status",
                                "status", "created_at", "updated_at", "update_ts", "company_size",
                                "tax_code"]
                            df = self.drop_cols(df, cols)
                            insert_df_to_postgres(
                                self.postgres_conf, tbl_name="adv2_employer",
                                df=df, primary_keys=['id']
                            )
                        elif k == "employer_freemium":
                            cols = [
                                "id", "employer_id", "channel_code", "status", "updated_at",
                                "created_at", "tax_code", "is_new"]
                            df = self.drop_cols(df, cols)
                            insert_df_to_postgres(
                                self.postgres_conf, tbl_name="adv2_employer_freemium",
                                df=df, primary_keys=['id']
                            )
                        elif k == "running_job_box":
                            insert_df_to_postgres(
                                self.postgres_conf, tbl_name="adv2_job_running",
                                df=df, primary_keys=['channel_code', 'job_id']
                            )
                        elif k == "registration_job_box":
                            insert_df_to_postgres(
                                self.postgres_conf, tbl_name="adv2_registration_job_box",
                                df=df, primary_keys=['id']
                            )
                        else:
                            raise Exception("not found source log")

            _current = time.time()
            if _current > self._next_check:
                _now = datetime.datetime.now().replace(tzinfo=local_tz)
                self.log.info(
                    "current at {0}, {1} processed ids: {2}".format(
                        _now, len(processed), str(processed)
                    )
                )
                self._next_check = _current + self.interval_check
                processed = list()

            if _current > self._next_auth:
                _now = datetime.datetime.now().replace(tzinfo=local_tz)
                self.get_token()
                self.log.info(
                    "current at {0}, gen new token".format(
                        _now
                    )
                )
                self._next_auth = _current + self.interval_auth_check
