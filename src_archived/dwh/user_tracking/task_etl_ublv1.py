import datetime
import pandas as pd
from dwh.user_tracking.user_tracking_base import UserTrackingBase
import pytz
from libs.storage_utils import down_gcs_to_df, get_gcs_cli, load_sa_creds, get_bq_cli
import json
from user_agents import parse as parse_ua
from urllib.parse import urlsplit, unquote
import bson

sg_tz = pytz.timezone("Asia/Saigon")
utc_tz = pytz.utc


class TaskEtlUblv1(UserTrackingBase):

    def __init__(self, config):
        super(TaskEtlUblv1, self).__init__(config)
        dp_ops = self.get_param_config(['dp-ops'])
        self.gcs_cli = get_gcs_cli(dp_ops)
        self.bq_cli = get_bq_cli(dp_ops)
        self.dp_ops = self.get_param_config(['dp-ops'])
        self.sa_creds = load_sa_creds(self.dp_ops)
        self.bucket_name = self.get_param_config(['bucket_name'])
        self.task_info = self.get_param_config(['task-info'])
        self.bq_target_table = self.get_param_config(['task-info', 'bq_target_table'])
        self.from_date = self.from_date.astimezone(sg_tz).replace(minute=0, second=0, microsecond=0)
        self.to_date = self.to_date.astimezone(sg_tz).replace(minute=0, second=0, microsecond=0)
        
    @staticmethod
    def parse_view_obj(obj):
        if isinstance(obj, dict):
            return obj.get('id', -1), obj.get('type', '').lower(), obj['info']
        return -1, "page", {}

    @staticmethod
    def parse_user_agent(obj):
        user_agent = parse_ua(obj)
        return user_agent.browser.family, \
            user_agent.browser.version_string, \
            user_agent.os.family, \
            user_agent.os.version_string, \
            user_agent.device.family, \
            user_agent.device.brand, \
            user_agent.device.model, \
            user_agent.is_mobile, \
            user_agent.is_tablet, \
            user_agent.is_pc, \
            user_agent.is_bot

    @staticmethod
    def parse_search_obj(obj):
        rename = {
            "keywordSearch": "keyword_search",
            "fieldSearch": "industry_search",
            "provinceSearch": "province_search",
            "levelSearch": "level_search",
            "experienceSearch": "experience_search",
            "salarySearch": "salary_search",
            "rankSearch": "level_search",
            "typeJobSearch": "type_job_search",
            "genderSearch": "gender_search",
            "languageSearch": "language_search"
        }
        obj_info = {rename.get(k, k): v for k, v in obj['info'].items()}

        return obj['id'], obj['type'].lower(), obj_info

    @staticmethod
    def parse_apply_job_obj(obj):
        rename = {
            "typeApply": "type_apply",
            "idResume": "resume_id",
            "idJob": "job_id",
            "id_job": "job_id",
            "id_resume_apply": "resume_id",
        }
        obj_info = {rename.get(k, k): v for k, v in obj['info'].items()}
        # obj_info = {k: v for k, v in obj_info.items() if k in ["type_apply", "job_id", "resume_id"]}

        return obj['id'], "resume_apply_job", obj_info

    @staticmethod
    def parse_open_contact_resume(obj):
        rename = {
            "id_employer": "employer_id",
            "id_resume": "resume_id",
            "typeOfResume": "type_of_resume",
            "pointUsed": "point_used",
            "idEmployer": "employer_id",
            "idResume": "resume_id"
        }
        obj_info = {rename.get(k, k): v for k, v in obj['info'].items()}
        # obj_info = {k: v for k, v in obj_info.items() if k in
        #             ["employer_id", "resume_id", "type_of_resume", "point_used"]}

        return obj['id'], "open_contact_resume", obj_info

    @staticmethod
    def parse_signup(obj):
        if isinstance(obj, dict):
            rename = {
                "dateOfBirth": "date_of_birth",
                "nameOfCompany": "name_of_company",
                "fieldOperation": "field_operation",
                "maritalStatus": "marital_status",
            }
            obj_info = {rename.get(k, k): v for k, v in obj['info'].items()}
            return obj['id'], obj.get('type', obj['info'].get('role')), obj_info
        else:
            return None, None, {}

    @staticmethod
    def parse_create_resume(obj):
        return obj['id'], obj['type'], obj['info']

    @staticmethod
    def parse_company_page(obj):
        return obj.get('id', -1), obj.get('type'), obj.get('info')

    @staticmethod
    def parse_resume_step(obj):
        return obj.get('id', -1), obj.get('type', 'resume_step'), obj.get('info')
        
    @classmethod
    def parse_raw(cls, raw):
        try:
            if not isinstance(raw, dict):
                return None
            verb = raw['verb']

            # action
            _action = raw['action']
            if not isinstance(_action, dict):
                return None
            tracking_id = _action['trackingId']
            action_id = _action['actionId']
            channel_code = tracking_id.split('-')
            if tracking_id == 'SV-2021':
                channel_code = 'SV'
            else:
                if len(channel_code) > 1:
                    channel_code = channel_code[1].upper()
                else:
                    channel_code = channel_code[0].upper()

            # subject
            _subject = raw['subject']
            if not isinstance(_subject, dict):
                return None
            subject_client_id = _subject['clientId']
            subject_device_id = _subject['deviceId']
            subject_exp_session_id = _subject.get('expSessionId')
            subject_exp_device_id = _subject.get("expDeviceId")
            subject_id = _subject['id']
            subject_type = _subject['type'].lower()

            object_id, object_type, object_info = None, None, None
            _object = raw['object']
            # if not isinstance(_object, dict):
            #     return None

            if verb in ["view_page", "page_view", "view_job",
                        "view_employer", "view_resume",
                        "click_popup_cv_builder", "click_menu_cv_builder",
                        "click", "cvb_click"]:
                object_id, object_type, object_info = cls.parse_view_obj(_object)
            if verb in ["search_resume", "search_job"]:
                object_id, object_type, object_info = cls.parse_search_obj(_object)
            if verb in ["apply_job"]:
                object_id, object_type, object_info = cls.parse_apply_job_obj(_object)
            if verb in ["open_contact_resume"]:
                object_id, object_type, object_info = cls.parse_open_contact_resume(_object)
            if verb in ["sign_up_seeker", "sign_up_employer"]:
                object_id, object_type, object_info = cls.parse_signup(_object)
            if verb in ["create_resume", ]:
                object_id, object_type, object_info = cls.parse_create_resume(_object)
            if verb.startswith("cp_"):
                object_id, object_type, object_info = cls.parse_company_page(_object)
            if verb.startswith("rs_"):
                object_id, object_type, object_info = cls.parse_resume_step(_object)

            if object_id is None:
                return None
            object_type = object_type.lower()
            verb = {"page_view": "view_page"}.get(verb, verb)

            _attributes = raw.get('attributes', dict())

            # marketing
            _param_mkt = _attributes.get('paramsMarketing')
            if not pd.isna(_param_mkt):
                mkt_source = _param_mkt['source']
                mkt_medium = _param_mkt['medium']
                mkt_campaign = _param_mkt['campaign']
                mkt_content = _param_mkt['content']
            else:
                mkt_source = None
                mkt_medium = None
                mkt_campaign = None
                mkt_content = None

            # service
            _param_service = _attributes.get('service')
            if isinstance(_param_service, dict) and not pd.isna(_param_service):
                service_id = _param_service.get('id')
                service_source = _param_service.get('source')
                service_campaign = _param_service.get('campaign')
                service_sub_campaign = _param_service.get('subCampaign', _param_service.get("sub_campaign", ''))
                service_box = _param_service.get('box')
            else:
                service_id = None
                service_source = None
                service_campaign = None
                service_sub_campaign = None
                service_box = None

            # page, referrer, other
            page_url = _attributes['page']['url']
            page_path = unquote(urlsplit(page_url).path)
            page_title = _attributes['page']['title'].replace(chr(0), '')
            referrer_url = _attributes['referrer']['url']
            _user_agent = _attributes['userAgent']
            user_agent_browser_family, \
                user_agent_browser_version, \
                user_agent_os_family, \
                user_agent_os_version, \
                user_agent_device_family, \
                user_agent_device_brand, \
                user_agent_device_model, \
                user_agent_is_mobile, \
                user_agent_is_tablet, \
                user_agent_is_pc, \
                user_agent_is_bot = cls.parse_user_agent(_user_agent)

            if page_path == '/':
                is_homepage = 1
            else:
                is_homepage = 0

            lst_search_page = ["/vieclam/timkiem", "/tim-kiem-viec-lam-nhanh", "/ho-so-tim-viec/tim-kiem",
                               "/tim-kiem-ung-vien-nhanh", "/viec-lam/tim-kiem", "/tim-viec-lam-24h",
                               "/tim-kiem-viec-lam-nhanh/", "/tim-kiem-ung-vien-nhanh/"]

            lst_adv_search_page = ["/viec-lam/tim-kiem-nang-cao", "/tim-kiem-viec-lam-nang-cao"]
            if page_path in lst_search_page:
                is_search_page = 1
            else:
                is_search_page = 0

            if page_path in lst_adv_search_page:
                is_adv_search_page = 1
            else:
                is_adv_search_page = 0

            point_used = object_info.get("point_used", 0.0)
        except KeyError as e:
            print(e, raw)
            return None

        return {
            "tracking_id": tracking_id,
            "action_id": action_id,
            "channel_code": channel_code,

            "subject_client_id": subject_client_id,
            "subject_device_id": subject_device_id,
            "subject_exp_session_id": subject_exp_session_id,
            "subject_exp_device_id": subject_exp_device_id,
            "subject_id": subject_id,
            "subject_type": subject_type,

            "verb": verb,

            "object_id": object_id,
            "object_type": object_type,
            "object_info": object_info,
            "attributes": _attributes,

            "mkt_source": mkt_source,
            "mkt_medium": mkt_medium,
            "mkt_campaign": mkt_campaign,
            "mkt_content": mkt_content,

            "service_id": service_id,
            "service_source": service_source,
            "service_campaign": service_campaign,
            "service_sub_campaign": service_sub_campaign,
            "service_box": service_box,

            "user_agent": _user_agent,
            "user_agent_browser_family": user_agent_browser_family,
            "user_agent_browser_version": user_agent_browser_version,
            "user_agent_os_family": user_agent_os_family,
            "user_agent_os_version": user_agent_os_version,
            "user_agent_device_family": user_agent_device_family,
            "user_agent_device_brand": user_agent_device_brand,
            "user_agent_device_model": user_agent_device_model,
            "user_agent_is_mobile": user_agent_is_mobile,
            "user_agent_is_tablet": user_agent_is_tablet,
            "user_agent_is_pc": user_agent_is_pc,
            "user_agent_is_bot": user_agent_is_bot,

            "page_url": page_url,
            "page_title": page_title,
            "page_path": page_path,
            "referrer_url": referrer_url,
            "is_homepage": is_homepage,
            "is_search_page": is_search_page,
            "is_adv_search_page": is_adv_search_page,
            "point_used": point_used
        }

    @staticmethod
    def upsert(current_df, new_df, primary_key):
        df_rs = pd.concat([current_df[~current_df[primary_key].isin(new_df[primary_key])], new_df])   
        return df_rs
     
    def extract_hourly(self, dt):
        gcs_path = "user_tracking/ublv1/{}".format(
            dt.strftime("%Y-%m-%d/%H%M.pq")
         )
        df = down_gcs_to_df(self.gcs_cli, self.bucket_name, gcs_path=gcs_path)
        return df
       
    def transform(self, df):
        df['ts'] = df['created_at'].dt.floor('h')
        df_base = pd.DataFrame(df['raw'].map(json.loads).map(self.parse_raw).dropna().tolist())
        df_base = df_base.join(df[['_id', 'ts', 'created_at', 'local_created_at']])
        df_base['created_at'] = df_base['_id'].apply(lambda x: bson.ObjectId(x).generation_time)
        df_base['local_created_at'] = df_base['created_at'] + datetime.timedelta(hours=7)
        df_base['attributes'] = df_base['attributes'].map(lambda x: json.dumps(x, ensure_ascii=False))
        df_base['object_info'] = df_base['object_info'].map(lambda x: json.dumps(x, ensure_ascii=False))
        df_base['subject_id'] = df_base['subject_id'].fillna('').map(str)
        df_base['object_id'] = df_base['object_id'].fillna('').map(str)
        df_base['service_id'] = df_base['service_id'].fillna('').map(str)
        df_base['service_source'] = df_base['service_source'].fillna('').map(str)
        df_base['service_campaign'] = df_base['service_campaign'].fillna('').map(str)
        df_base['service_sub_campaign'] = df_base['service_sub_campaign'].fillna('').map(str)
        return df_base

    def load(self, df):
        sql = "DELETE FROM {0} WHERE created_at >= '{1}' and created_at <'{2}' ".format(
            self.bq_target_table, self.from_date, self.to_date)
        self.log.info("sql delete: {}".format(sql))
        bq_cli = self.bq_cli
        query_job = bq_cli.query(sql)
        query_job.result()
        
        schema = [
            {'name': 'tracking_id', 'type': 'STRING'},
            {'name': 'action_id', 'type': 'STRING'},
            {'name': 'channel_code', 'type': 'STRING'},
            {'name': 'subject_client_id', 'type': 'STRING'},
            {'name': 'subject_device_id', 'type': 'STRING'},
            {'name': 'subject_exp_session_id', 'type': 'STRING'},
            {'name': 'subject_exp_device_id', 'type': 'STRING'},
            {'name': 'subject_id', 'type': 'STRING'},
            {'name': 'subject_type', 'type': 'STRING'},
            {'name': 'verb', 'type': 'STRING'},
            {'name': 'object_id', 'type': 'STRING'},
            {'name': 'object_type', 'type': 'STRING'},
            {'name': 'object_info', 'type': 'STRING'},
            {'name': 'attributes', 'type': 'STRING'},
            {'name': 'mkt_source', 'type': 'STRING'},
            {'name': 'mkt_medium', 'type': 'STRING'},
            {'name': 'mkt_campaign', 'type': 'STRING'},
            {'name': 'mkt_content', 'type': 'STRING'},
            {'name': 'service_id', 'type': 'STRING'},
            {'name': 'service_source', 'type': 'STRING'},
            {'name': 'service_campaign', 'type': 'STRING'},
            {'name': 'service_sub_campaign', 'type': 'STRING'},
            {'name': 'service_box', 'type': 'STRING'},
            {'name': 'user_agent', 'type': 'STRING'},
            {'name': 'user_agent_browser_family', 'type': 'STRING'},
            {'name': 'user_agent_browser_version', 'type': 'STRING'},
            {'name': 'user_agent_device_family', 'type': 'STRING'},
            {'name': 'user_agent_device_braid', 'type': 'STRING'},
            {'name': 'user_agent_device_model', 'type': 'STRING'},
            {'name': 'user_agent_is_mobile', 'type': 'BOOLEAN'},
            {'name': 'user_agent_is_tablet', 'type': 'BOOLEAN'},
            {'name': 'user_agent_is_pc', 'type': 'BOOLEAN'},
            {'name': 'user_agent_is_bot', 'type': 'BOOLEAN'},
            {'name': 'page_url', 'type': 'STRING'},
            {'name': 'page_title', 'type': 'STRING'},
            {'name': 'page_path', 'type': 'STRING'},
            {'name': 'referrer_url', 'type': 'STRING'},
            {'name': 'is_homepage', 'type': 'INTEGER'},
            {'name': 'is_search_page', 'type': 'INTEGER'},
            {'name': 'is_adv_search_page', 'type': 'INTEGER'},
            {'name': 'point_used', 'type': 'FLOAT'},
            {'name': '_id', 'type': 'STRING'},
            {'name': 'ts', 'type': 'timestamp'},
            {'name': 'created_at', 'type': 'timestamp'},
            {'name': 'local_created_at', 'type': 'timestamp'},
            {'name': 'event_created_at', 'type': 'timestamp'},
        ]
        df.to_gbq(
            self.bq_target_table, if_exists="append",
            table_schema=schema, credentials=self.sa_creds)

    def execute(self):
        df = self.extract_hourly(self.from_date)
        df = self.transform(df)
        self.load(df)
        
    def backfill(self, interval_hours=1):
        from_date = self.from_date
        to_date = self.to_date
        seed_date = from_date
        while seed_date <= to_date:
            self.from_date = from_date
            self.to_date = seed_date
            print(self.from_date, self.to_date)
            self.execute()
            seed_date += datetime.timedelta(hours=interval_hours)
            self.from_date = seed_date
