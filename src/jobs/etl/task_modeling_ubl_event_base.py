import tqdm
from dateutil import tz
import datetime
import pandas as pd
import json
from urllib.parse import urlsplit, unquote
from core.app_base import AppBase
from libs.storage_utils import load_sa_creds, get_bq_cli
from user_agents import parse as parse_ua


local_tz = tz.tzlocal()
utc_tz = tz.tzutc()
tqdm.tqdm.pandas()


class TaskModelingUblEventBase(AppBase):

    def __init__(self, config):
        super(TaskModelingUblEventBase, self).__init__(config)
        self.gcp_sv_data_account_key_path = self.get_param_config(['gcp_sv_data_account_key_path'])
        self.table_source = self.get_param_config(['table_source'])
        self.table_base = self.get_param_config(['table_base'])
        self.bq_creds = load_sa_creds(self.gcp_sv_data_account_key_path)
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

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

    def extract(self):
        from_time = self.from_date - datetime.timedelta(hours=1)
        to_time = self.to_date - datetime.timedelta(hours=1)
        sql = "SELECT TIMESTAMP_TRUNC(created_at, HOUR) as ts, _id, created_at, local_created_at, raw " \
              "FROM {} WHERE created_at >= '{}' AND created_at < '{}'".format(self.table_source, from_time, to_time)
        self.log.info("raw sql: {}".format(sql))
        df_raw = pd.read_gbq(sql, credentials=self.bq_creds)
        return df_raw

    def transform(self, df_raw):
        df_base = pd.DataFrame(df_raw['raw'].map(json.loads).map(self.parse_raw).dropna().tolist())
        df_base = df_base.join(df_raw[['_id', 'created_at', 'ts', 'local_created_at']])
        df_base['_id'] = df_base['_id'].map(str)
        df_base['attributes'] = df_base['attributes'].map(lambda x: json.dumps(x, ensure_ascii=False))
        df_base['object_info'] = df_base['object_info'].map(lambda x: json.dumps(x, ensure_ascii=False))
        df_base['subject_id'] = df_base['subject_id'].fillna('').map(str)
        df_base['object_id'] = df_base['object_id'].fillna('').map(str)
        df_base['service_id'] = df_base['service_id'].fillna('').map(str)
        df_base['service_source'] = df_base['service_source'].fillna('').map(str)
        df_base['service_campaign'] = df_base['service_campaign'].fillna('').map(str)
        df_base['service_sub_campaign'] = df_base['service_sub_campaign'].fillna('').map(str)
        return df_base

        # # step 3: sub obj
        # df_block_sub_obj = df_base.groupby([
        #     'ts', 'channel_code', 'subject_client_id',
        #     'subject_device_id', 'subject_id', 'subject_type', 'verb', 'object_id',
        #     'object_type'
        # ]).agg({'tracking_id': 'count', 'created_at': [min, max]}).reset_index()
        # df_block_sub_obj.columns = ["".join(cols) for cols in df_block_sub_obj.columns]
        # df_block_sub_obj.rename(columns={
        #     "tracking_idcount": "num_track",
        #     "created_atmin": "first_created_at",
        #     "created_atmax": "last_created_at"
        # })
        #
        # df_object = df_base[['channel_code', 'object_id', 'object_id', 'object_info']].drop_duplicates()
        #
        # # step 4: sub service
        # df_block_sub_service = df_base.loc[df_base['service_source'] != ''].groupby([
        #     'ts', 'channel_code', 'subject_client_id',
        #     'subject_device_id', 'subject_id', 'subject_type', 'verb', 'object_id',
        #     'object_type', 'service_source'
        # ])['tracking_id'].count().reset_index()
        #
        # df_block_sub_mkt = df_base.loc[df_base['mkt_source'] != ''].groupby([
        #     'ts', 'channel_code', 'subject_client_id',
        #     'subject_device_id', 'subject_id', 'subject_type', 'verb', 'object_id',
        #     'object_type', 'mkt_source', 'mkt_medium', 'mkt_campaign', 'mkt_content'
        # ])['tracking_id'].count().reset_index()
        #
        # df_block_obj_service = df_base.loc[df_base['service_source'] != ''].groupby([
        #     'ts', 'channel_code', 'object_id', 'object_type',
        #     'verb', 'subject_type', 'service_source'
        # ])['tracking_id'].count().reset_index()
        #
        # df_block_obj_mkt = df_base.loc[df_base['mkt_source'] != ''].groupby([
        #     'ts', 'channel_code', 'object_id', 'object_type',
        #     'verb', 'subject_type', 'mkt_source', 'mkt_medium', 'mkt_campaign', 'mkt_content'
        # ])['tracking_id'].count().reset_index()
        #
        # return df_base, df_block_sub_obj, df_object, \
        #     df_block_sub_service, df_block_sub_mkt, \
        #     df_block_obj_service, df_block_obj_mkt

    def load(self, df_base):
        from_time = self.from_date - datetime.timedelta(hours=1)
        to_time = self.to_date - datetime.timedelta(hours=1)
        sql = "DELETE FROM {0} WHERE created_at >= '{1}' and created_at <'{2}' ".format(
            self.table_base, from_time, to_time)
        self.log.info("sql delete: {}".format(sql))
        bq_cli = get_bq_cli(self.gcp_sv_data_account_key_path)
        query_job = bq_cli.query(sql)
        query_job.result()
        df_base.to_gbq(self.table_base, if_exists="append", credentials=self.bq_creds)

    def execute(self):
        # step 1: extract data
        self.log.info("# step 1: extract data")
        df_raw = self.extract()
        if df_raw.shape[0] == 0:
            self.log.warning("raw empty")
            return

        # step 2: etl
        self.log.info("# step 2: etl")
        df_base = self.transform(df_raw)

        # step 3: load
        self.log.info("# step 3: load")
        self.load(df_base)

    def backfill(self, interval_hours=6):
        from_date = self.from_date
        to_date = self.to_date
        seed_date = from_date + datetime.timedelta(hours=interval_hours)
        while seed_date <= to_date:
            self.from_date = from_date
            self.to_date = seed_date
            print(self.from_date, self.to_date)
            self.execute()
            from_date = seed_date
            seed_date += datetime.timedelta(hours=interval_hours)
