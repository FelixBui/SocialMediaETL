import pandas as pd
import requests
from core.app_base import AppBase
from dateutil import tz
from dateutil.parser import parse as parse_dt

from libs.storage_utils import load_df_from_mongo, insert_df_to_postgres
from consolidated.normalize import norm_areas

local_tz = tz.tzlocal()


class TaskCentralizeVnwJobV2(AppBase):
    mapping_vnw_job = {
        'url_job': 'url',
        'jobTitle': 'name',
        'expiredDate': 'expired_at',
        'publishedDate': 'publish_at',
        'locationVIs': 'areas',
        'locationIds': 'area_ids',
        'onlineDate': 'online_at',
        'jobId': 'id',
        'companyId': 'company_id',
        'benefits': 'benefits',
        'jobLocations': 'location',
        'company': 'company_name',
        'job_requirement': 'requirement',
        'job_description': 'description',
        'industries': 'job_fields',  # industry --> job_fields, new v2
        # 'categoryIds': 'job_field_ids',  # industry_ids --> job_field_ids
        'expiredOn': 'expired_at',  # new v2
        'approvedOn': 'approved_at',  # new v2
        'onlineOn': 'online_at',  # new v2
        'workingLocations': 'areas',  # new v2
        'salaryMax': 'salary_max',
        'salaryMin': 'salary_min',
        'jobLevelVI': 'level_name',
        'alias': 'url_alias',
        'timestamp': 'job_updated_at',
        'userId': 'user_id',
        'isPremium': 'is_premium',
        'isUrgentJob': 'is_urgent_job',
        'isManagementJob': 'is_management_job',
        'isPriorityJob': 'is_priority_job',
        'isMobileHotJob': 'is_mobile_hot_job',
        'isMobileTopJob': 'is_mobile_top_job',
        'isBoldAndRedJob': 'is_bold_and_red_job',
        'isHeadhuntJob': 'is_headhunt_job',
        'priorityOrder': 'priority_order',
        'serviceCode': 'service_code',
        'mobileTopPriorityOrder': 'mobile_top_priority_order',
        'isTopManagementJob': 'is_top_management_job'
    }
    vnw_job_cols = [
        'objectID', 'id', "_id", 'company_id', 'name', 'user_id', 'areas', 'area_ids',
        'benefit_names', 'benefit_ids', 'job_fields', "job_field_ids",
        'level_name', 'location', 'requirement', 'description', 'skills', 'requirement',
        'salary_max', 'salary_min', 'url_alias', 'url', 'sourcetype', 'service_code',
        'expired_at', 'job_updated_at', 'online_at', "publish_at", "updated_at", "created_at",
        'is_premium', 'is_urgent_job', 'priority_order', 'is_management_job',
        'is_priority_job', 'is_mobile_hot_job', 'is_mobile_top_job', 'is_bold_and_red_job',
        'mobile_top_priority_order', 'is_top_management_job', 'is_headhunt_job', 'job_view'
    ]

    vnw_meta_col = [
        'id', 'updated_at',
        'is_premium', 'is_urgent_job', 'is_management_job', 'is_priority_job',
        'is_mobile_hot_job', 'is_mobile_top_job', 'is_bold_and_red_job',
        'is_top_management_job', 'is_headhunt_job'
    ]

    mapping_vnw_company = {
        'companyId': 'id',
        'companySlug': 'vnw_slug',
        'followerCount': 'follower_count',
        'keyTechnologies': 'key_technologies',
        'categoryIds': 'category_ids',
        'categories_vi': 'categories',
        'locations_vi': 'areas',
        'publishedSites': 'published_sites',
        'companyDesc': 'description',
        'companyAddress': 'address',
        'companyProfileType': 'profile_type',
        'firstPublishedOn': 'first_published_on',
        'lastPublishedOn': 'last_published_on',
        'viewCount': 'view_count',
        'onlineJobCount': 'online_job_count',
        'locationIds': 'area_ids'
    }

    vnw_company_col = [
        'objectID', 'id', '_id', 'name', 'size', 'vnw_slug', 'benefit_names', 'benefit_ids',
        'follower_count', 'key_technologies', 'category_ids', 'categories', 'areas',
        'published_sites', 'description', 'address', 'profile_type',
        'first_published_on', 'last_published_on', 'view_count',
        'online_job_count', 'area_ids', 'sourcetype', 'updated_at', 'created_at'
    ]

    map_size_name = {
        0: "Không xác định",
        1: "Dưới 20 người",
        2: "20 - 150 người",
        3: "150 - 300 người",
        4: "Trên 300 người"
    }

    def __init__(self, config):
        super(TaskCentralizeVnwJobV2, self).__init__(config)
        self.mongo_conf = self.get_param_config(['db', 'mg-competitor'])
        self.postgres_conf = self.get_param_config(['db', 'pg-competitor'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

    @staticmethod
    def parse_benefit_job_vnw(x):
        benefit_names = list()
        benefit_ids = list()
        if isinstance(x, list):
            for e in x:
                benefit_names.append(e['benefitName'])
                benefit_ids.append(e['benefitId'])
        return {"benefit_names": benefit_names, "benefit_ids": benefit_ids}

    @staticmethod
    def get_vnw_benefits(x):
        vi = x['benefits_vi']
        en = x['benefits_en']
        benefit_names = list()
        benefit_ids = list()
        if isinstance(vi, list):
            for e in vi:
                benefit_names.append(e['coBenefitName'])
                benefit_ids.append(e['coBenefitId'])
        elif isinstance(en, list):
            for e in en:
                benefit_names.append(e['coBenefitName'])
                benefit_ids.append(e['coBenefitId'])
        return {"benefit_names": benefit_names, "benefit_ids": benefit_ids}

    @staticmethod
    def extract_feature_jobs():
        r = requests.get(
            "https://ms.vietnamworks.com/premium-jobs/v1.0/featured-jobs",
            proxies={
                "http": "http://torproxy-proxy2.torproxy:8118",
                "https": "http://torproxy-proxy2.torproxy:8118"
            },
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                          "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "max-age=0",
                "Connection": "keep-alive",
                "Host": "ms.vietnamworks.com",
                "sec-ch-ua": 'Not;A Brand";v="99", "Microsoft Edge";v="103", "Chromium";v="103"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "macOS",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.134 "
                              "Safari/537.36 Edg/103.0.1264.71"
            }
        )
        if r.status_code == 200:
            return pd.DataFrame(
                r.json()['data']
            ).reset_index()
        else:
            return pd.DataFrame([], columns=['id', 'is_home_featured'])

    def extract(self):
        query = {
            "updated_at": {
                "$gte": self.from_date,
                "$lt": self.to_date
            }
        }
        df_raw_vnw_job = load_df_from_mongo(self.mongo_conf, collection_name="vnw_job_v2", query=query, no_id=False)
        df_raw_vnw_job['created_at'] = df_raw_vnw_job['_id'].map(lambda x: x.generation_time.astimezone(local_tz))

        df_benefit_job_vnw = pd.DataFrame(df_raw_vnw_job['benefits'].map(self.parse_benefit_job_vnw).tolist())
        df_raw_vnw_job = df_raw_vnw_job.join(df_benefit_job_vnw)

        df_vnw_feature_jobs = self.extract_feature_jobs()
        df_vnw_feature_jobs['id'] = df_vnw_feature_jobs['id'].map(str)
        df_vnw_feature_jobs['is_home_featured'] = True
        df_vnw_feature_jobs = df_vnw_feature_jobs[['id', 'is_home_featured']]

        return df_raw_vnw_job, df_vnw_feature_jobs

    @staticmethod
    def get_vnw_size_min_max(x, pos=0):
        if x is None:
            return None
        x = x.replace('.', '').replace(',', '').replace(
            'Hơn 50000', '50000-999999').replace("Ít hơn 10", '0-9').split('-')
        if len(x) > 1:
            return int(x[pos])

    @staticmethod
    def norm_size(x_max):
        range_size = [20, 150, 300]
        if (x_max is None) or pd.isna(x_max):
            return 0
        for ix, v in enumerate(range_size):
            if x_max < v:
                return ix + 1
        else:
            return len(range_size) + 1

    def transform(self, df_raw_vnw_job, df_vnw_feature_jobs):
        df_vnw_job = df_raw_vnw_job.rename(columns=self.mapping_vnw_job)
        for col in self.vnw_job_cols:
            if col not in df_vnw_job.columns:
                df_vnw_job[col] = None

        df_vnw_job['company_id'] = df_vnw_job['company_id'].astype(str)
        df_vnw_job['user_id'] = df_vnw_job['user_id'].astype(str)

        df_vnw_job['id'] = df_vnw_job['id'].map(str)
        df_vnw_job['expired_at'] = df_vnw_job['expired_at'].map(
            lambda x: parse_dt(x))
        df_vnw_job['online_at'] = df_vnw_job['online_at'].map(
            lambda x: parse_dt(x))
        df_vnw_job['priority_order'] = df_vnw_job['priority_order'].map(
            lambda x: parse_dt(x).timestamp())
        # df_vnw_job['publish_at'] = df_vnw_job['publish_at'].map(
        #     lambda x: parse_dt(x))
        # df_vnw_job['job_updated_at'] = df_vnw_job['job_updated_at'].map(
        #     lambda x: parse_dt(x))

        # areas
        df_vnw_job['area_ids'] = df_vnw_job['areas'].map(lambda x: [e['cityId'] for e in x])
        df_vnw_job['areas'] = df_vnw_job['areas'].map(lambda x: [e['cityNameVI'] for e in x]).map(norm_areas)

        # job fields
        df_vnw_job['job_field_ids'] = df_vnw_job['job_fields'].map(lambda x: [e['industryId'] for e in x])
        df_vnw_job['job_fields'] = df_vnw_job['job_fields'].map(lambda x: [e['industryNameVI'] for e in x])
        # print(df_vnw_job['job_fields'])
        # skills
        df_vnw_job['skills'] = df_vnw_job['skills'].map(
            lambda x: [e['skillName'] for e in x] if isinstance(x, list) else x)

        # get created_at from _id
        df_vnw_job = df_vnw_job[self.vnw_job_cols]

        # create id for vnw meta
        df_vnw_meta = df_vnw_job[self.vnw_meta_col].copy().rename(columns={
            "id": "job_id"
        })
        str_time = self.execution_date.strftime("%Y%m%d_")
        df_vnw_meta["id"] = df_vnw_meta["job_id"].map(lambda x: str_time + x)
        df_vnw_meta = df_vnw_meta.join(df_vnw_feature_jobs.set_index('id'), 'job_id').fillna(False)

        # company_df = df_raw_vnw_job.rename(
        #     columns=self.mapping_vnw_job)[['company_id', 'company_info', 'company_intro', 'updated_at', 'created_at']]
        # company_df_info = pd.DataFrame(company_df['company_info'].tolist())
        # if 'id' in company_df_info.columns:
        #     company_df_info.drop(columns=['id'], inplace=True)
        # company_df = company_df.join(
        #     company_df_info
        # ).drop(columns=['company_info']).sort_values('created_at').drop_duplicates(['company_id']).rename(
        #     columns={'company_id': 'id', 'desc': 'description', 'location': 'address'}
        # )
        # company_df['id'] = company_df['id'].map(str)
        # company_ids = company_df['id'].map(lambda x: "'{}'".format(x)).tolist()
        # q = """select id from vnw_company where id in ({})""".format(",".join(company_ids))
        # tmp_df = load_df_from_postgres(self.postgres_conf, q)
        # if tmp_df.shape[0] > 0:
        #     company_df = company_df.loc[
        #         ~company_df['id'].isin(tmp_df['id'])
        #     ].reset_index(drop=True)
        # if company_df.shape[0] > 0:
        #     # company_df['is_brand'] = company_df['id'].map(
        #     #     lambda x: True if x in df_vnw_brand_company['id'].tolist() else False)
        #
        #     # norm size
        #     company_df['size_min'] = company_df['size'].map(lambda x: self.get_vnw_size_min_max(x, 0))
        #     company_df['size_max'] = company_df['size'].map(lambda x: self.get_vnw_size_min_max(x, 1))
        #     company_df['size_code'] = company_df['size_max'].map(self.norm_size)
        #     company_df['size_name'] = company_df['size_code'].map(self.map_size_name)
        #
        #     # name
        #     company_df['name'] = company_df['name'].str.strip().str.lower()
        #     company_df['website'] = company_df['link'].map(norm_website)
        #     company_df['channel_code'] = 'VNW'
        #
        #     # tax code
        #     df_company_taxid = load_df_from_mongo(
        #         self.mongo_conf, 'company_lookup_result',
        #         query={"search_rank": 1}, selected_keys=["original_name", "taxid"]).rename(
        #         columns={"taxid": "tax_code"})
        #     df_company_taxid['tax_code'] = df_company_taxid['tax_code'].map(lambda x: x.split('-')[0])
        #
        #     company_df = company_df.join(df_company_taxid.set_index('original_name'), 'name')
        # company_df

        return df_vnw_job, df_vnw_meta

    def load(self, df_vnw_job, df_vnw_meta):
        df_vnw_job['_id'] = df_vnw_job['_id'].map(str)
        # print(df_vnw_job.iloc[0])
        insert_df_to_postgres(self.postgres_conf, tbl_name="vnw_job_v2",
                              df=df_vnw_job, primary_keys=['id'])
        insert_df_to_postgres(self.postgres_conf, tbl_name="vnw_meta", df=df_vnw_meta,
                              primary_keys=['id'])

    def execute(self):
        self.log.info("step extract")
        df_raw_vnw_job, df_vnw_feature_jobs = self.extract()
        self.log.info("step transform")
        df_vnw_job, df_vnw_meta = self.transform(
            df_raw_vnw_job, df_vnw_feature_jobs)
        self.log.info("step load")
        self.load(df_vnw_job, df_vnw_meta)
