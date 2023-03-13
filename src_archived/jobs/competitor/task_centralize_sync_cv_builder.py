from dateutil import tz
import pymongo
import ssl
import pandas as pd
from bson.objectid import ObjectId
from core.app_base import AppBase
from libs.storage_utils import insert_df_to_postgres

local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class TaskCentralizeSyncCvBuilder(AppBase):

    def __init__(self, config):
        super(TaskCentralizeSyncCvBuilder, self).__init__(config)
        mongo_uri = self.get_param_config(['ubl_mongo_uri'])
        self.db = pymongo.MongoClient(
            mongo_uri, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)['nest_js']

        self.postgres_conf = self.get_param_config(['db', 'db_dp'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

    def extract(self):
        from_id = ObjectId.from_datetime(self.from_date)
        to_id = ObjectId.from_datetime(self.to_date)
        df_raw = pd.DataFrame(list(
            self.db['usercvs'].find({
                "$or": [
                    {"_id": {"$gte": from_id, "$lt": to_id}},
                    {"updatedAt": {"$gte": self.from_date, "$lt": self.to_date}}
                ]
            })
        ))

        df_auths = pd.DataFrame(list(
            self.db['auths'].find({
                "$or": [
                    {"_id": {"$gte": from_id, "$lt": to_id}},
                    {"updatedAt": {"$gte": self.from_date, "$lt": self.to_date}}
                ]
            })
        ))
        if df_auths.shape[0] > 0:
            df_auths['_id'] = df_auths['_id'].map(str)
            df_auths = df_auths.rename(columns={
                "fullName": "full_name",
                "createdAt": "created_at",
                "updatedAt": "updated_at"
            })

        return df_raw, df_auths

    @staticmethod
    def transform(df_raw):
        df_user = pd.DataFrame(df_raw['userData'].map(lambda x: x['about']).tolist())
        df_user = df_user.rename(columns={
            "dateOfBirth": "dob",
            "phoneNumber": "phone",
            "positionApply": "position",
            "fullName": "full_name"
        })
        df_user['hobby'] = df_raw['userData'].map(lambda x: x['hobby'])

        rs = list()
        for ix, r in enumerate(df_raw['userData'].map(lambda x: x['skill'])):
            for i, v in enumerate(r):
                rs.append((ix, i, v))
        df_tmp = pd.DataFrame(rs, columns=['ix', 'no', 'tmp'])
        df_skills = pd.DataFrame(df_tmp['tmp'].tolist())
        df_skills = df_skills.join(df_tmp)

        rs = list()
        for ix, r in enumerate(df_raw['userData'].map(lambda x: x['experience'])):
            for i, v in enumerate(r):
                rs.append((ix, i, v))
        df_tmp = pd.DataFrame(rs, columns=['ix', 'no', 'tmp'])
        df = pd.DataFrame(df_tmp['tmp'].tolist())
        df = df.join(df_tmp)
        df_experience = df.rename(columns={
            "companyName": "company_name",
            "workingTime": "working_time"
        })

        rs = list()
        for ix, r in enumerate(df_raw['userData'].map(lambda x: x['certificate'])):
            for i, v in enumerate(r):
                rs.append((ix, i, v))
        df_tmp = pd.DataFrame(rs, columns=['ix', 'no', 'tmp'])
        df = pd.DataFrame(df_tmp['tmp'].tolist())
        df = df.join(df_tmp)
        df_certificate = df.rename(columns={
            "trainingCenter": "training_center",
        })

        rs = list()
        for ix, r in enumerate(df_raw['userData'].map(lambda x: x['education'])):
            for i, v in enumerate(r):
                rs.append((ix, i, v))
        df_tmp = pd.DataFrame(rs, columns=['ix', 'no', 'tmp'])
        df = pd.DataFrame(df_tmp['tmp'].tolist())
        df = df.join(df_tmp)
        df_education = df.rename(columns={
            "schoolName": "school_name",
        })

        if 'isMobile' not in df_raw.columns:
            df_raw['isMobile'] = None
        df_raw = df_raw[['_id', 'template', 'owner', 'title', 'html',
                         'isParsed', 'isMobile', 'createdAt', 'updatedAt']]
        df_raw = df_raw.rename(
            columns={
                "isParsed": "is_parsed",
                "createdAt": "created_at",
                "updatedAt": "updated_at",
                "isMobile": "is_mobile"
            }
        )

        df_raw['_id'] = df_raw['_id'].map(str)
        df_raw['template'] = df_raw['template'].map(str)
        df_raw['owner'] = df_raw['owner'].map(str)

        df_user = df_raw[['_id', 'template', 'owner', 'created_at', 'updated_at']].join(df_user)
        df_education = df_raw[['_id', 'template', 'owner', 'created_at', 'updated_at']].join(
            df_education.set_index(['ix'])).dropna(subset=['no']).drop(columns='tmp')
        df_certificate = df_raw[['_id', 'template', 'owner', 'created_at', 'updated_at']].join(
            df_certificate.set_index(['ix'])).dropna(subset=['no']).drop(columns='tmp')
        df_experience = df_raw[['_id', 'template', 'owner', 'created_at', 'updated_at']].join(
            df_experience.set_index(['ix'])).dropna(subset=['no']).drop(columns='tmp')
        df_skills = df_raw[['_id', 'template', 'owner', 'created_at', 'updated_at']].join(
            df_skills.set_index(['ix'])).dropna(subset=['no']).drop(columns='tmp')
        return df_raw, df_user, df_education, df_certificate, df_experience, df_skills

    def load(self, df_raw, df_user, df_education, df_certificate, df_experience, df_skills, df_auths):
        insert_df_to_postgres(
            self.postgres_conf,
            tbl_name="cv_entry",
            df=df_raw,
            primary_keys=['_id'],
            schema='cvbuilder',
        )

        insert_df_to_postgres(
            self.postgres_conf,
            tbl_name="cv_userdata",
            df=df_user,
            primary_keys=['_id'],
            schema='cvbuilder',
        )

        insert_df_to_postgres(
            self.postgres_conf,
            tbl_name="cv_education",
            df=df_education,
            primary_keys=['_id', 'no'],
            schema='cvbuilder',
        )

        insert_df_to_postgres(
            self.postgres_conf,
            tbl_name="cv_certificate",
            df=df_certificate,
            primary_keys=['_id', 'no'],
            schema='cvbuilder',
        )

        insert_df_to_postgres(
            self.postgres_conf,
            tbl_name="cv_experience",
            df=df_experience,
            primary_keys=['_id', 'no'],
            schema='cvbuilder',
        )

        insert_df_to_postgres(
            self.postgres_conf,
            tbl_name="cv_skills",
            df=df_skills,
            primary_keys=['_id', 'no'],
            schema='cvbuilder'
        )

        insert_df_to_postgres(
            self.postgres_conf,
            tbl_name="user",
            df=df_auths,
            primary_keys=['_id'],
            schema='cvbuilder'
        )

    def execute(self):
        df_raw, df_auths = self.extract()
        if df_raw.shape[0] > 0:
            df_raw, df_user, df_education, df_certificate, df_experience, df_skills = self.transform(df_raw)
            self.load(df_raw, df_user, df_education, df_certificate, df_experience, df_skills, df_auths)
        elif df_auths.shape[0] > 0:
            self.log.warning("raw empty")
            insert_df_to_postgres(
                self.postgres_conf,
                tbl_name="user",
                df=df_auths,
                primary_keys=['_id'],
                schema='cvbuilder'
            )
        else:
            self.log.warning("data not found")
