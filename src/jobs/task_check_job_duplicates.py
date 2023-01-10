from dateutil import tz
import re
import tqdm

from joblib import Parallel, delayed

import pandas as pd
from underthesea import word_tokenize
from google.cloud import bigquery
from google.oauth2 import service_account

from core.app_base import AppBase
from libs.db_utils import load_df_from_postgres, insert_df_to_postgres_fast
from consolidated.normalize import build_network_duplicates

local_tz = tz.tzlocal()


def has_numbers(s):
    return bool(re.search(r'\d', s))


def gen_global_company_id(x):
    if x['tax_code'] != '':
        return x['tax_code']
    else:
        return "{}_{}".format(x['channel_code'], x['company_id'])


def token_name(x):
    return [w for w in word_tokenize(x)
            if (w not in ['~', '!', '@', '#', '$', '%', '^', '&', '/', '`', '–',
                          '*', '(', ')', '_', '-', '+', '{', '}', '[', ']',
                          '|', ';', ':', "'", '<', '>', ',', '.', '?', '"']
                and not has_numbers(w))]


class TaskCheckJobDuplicates(AppBase):
    map_level = {
        "Nhân viên": "Nhân viên",
        "Quản lý": "Trưởng Phòng",
        "Trưởng nhóm / Giám sát": "Nhân viên",
        "Trưởng/Phó phòng": "Trưởng Phòng",
        "Mới tốt nghiệp": "Mới Tốt Nghiệp",
        "Sinh viên/ Thực tập sinh": "Mới Tốt Nghiệp",
        "Phó Giám đốc": "Giám Đốc và Cấp Cao Hơn",
        "Tổng giám đốc": "Giám Đốc và Cấp Cao Hơn",
        "Thực tập sinh": "Mới Tốt Nghiệp",
        "Trưởng nhóm": "Nhân viên",
        "Quản lý / Giám sát": "Nhân viên",
        "Trưởng chi nhánh": "Trưởng Phòng",
        "Phó giám đốc": "Giám Đốc và Cấp Cao Hơn",
        "Trường phòng": "Trưởng Phòng",
        "Quản lý cấp trung": "Trưởng Phòng",
        "Quản lý cấp cao": "Trưởng Phòng",
        "Giám đốc": "Giám Đốc và Cấp Cao Hơn",
        "Cộng tác viên": "Mới Tốt Nghiệp",
        "Trưởng phó phòng": "Trưởng Phòng",
        "CTV": "Mới Tốt Nghiệp",
        "Mới tốt nghiệp / Thực tập sinh": "Mới Tốt Nghiệp",
        "Tổng giám đốc điều hành": "Giám Đốc và Cấp Cao Hơn",
    }

    def __init__(self, config):
        super(TaskCheckJobDuplicates, self).__init__(config)
        self.postgres_conf = self.get_param_config(['db', 'postgres'])
        # self.from_date = self.get_process_info(['from_date'])
        # self.to_date = self.get_process_info(['to_date'])
        self.gcp_sv_data_account_key_path = self.get_param_config(['gcp_sv_data_account_key_path'])
        self.sv_mapping_path = self.get_param_config(['sv_mapping_path'])

    def extract(self):
        df_vnw_job = load_df_from_postgres(
            self.postgres_conf,
            query="""
            select job.id, company.tax_code, job.name, company_id, level_name as level, areas, job.description
            from vnw_job job
            inner join vnw_company company
            on job.company_id = company.id
            where job.created_at >= '2020-11-15'
            """
        )

        df_cb_job = load_df_from_postgres(
            self.postgres_conf,
            query="""
            select job.id, company.tax_code, job.name, company_id, level, areas, job.description
            from cb_job job
            inner join cb_company company
            on job.company_id = company.id
            where job.created_at >= '2020-11-15'
            """
        )

        df_topcv_job = load_df_from_postgres(
            self.postgres_conf,
            query="""
            select job.id, company.tax_code, job.name, company_id, level, areas, job.description
            from topcv_job job
            inner join topcv_company company
            on job.company_id = company.id
            where job.created_at >= '2020-11-15'
            """
        )

        credentials = service_account.Credentials.from_service_account_file(
            self.gcp_sv_data_account_key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
        df_raw_sv_job = client.query("""
        SELECT
            l.channel_code,
            CAST(l.id AS string) AS id,
            CAST(l.company_id AS string) AS company_id,
            l.name,
            l.level,
            l.description,
            l.province_ids,
            r.tax_code
        FROM
            (
                SELECT
                    con.channel_code,
                    id,
                    company_id,
                    name,
                    level,
                    description,
                    employer_id_key,
                    province_ids
                FROM
                    (
                        (
                        SELECT
                            DISTINCT channel_code,
                            job_id_ref
                        FROM
                            `data-warehouse-sv.raw_data.persisted_acc_job_queue`
                        WHERE
                            start_date >= "2020-11-15"
                        ) UNION ALL
                        (
                        SELECT
                            DISTINCT UPPER(channel_code) as channel_code,
                            job_id as job_ref_id
                        FROM
                            `raw_data.adminv2_registration_job_box`
                        WHERE
                            start_date >= "2020-11-15"
                        )
                    ) con
                    INNER JOIN (
                        SELECT
                            job_id_key,
                            channel_code,
                            ref_id as id,
                            employer_id as company_id,
                            title as name,
                            level_requirement_name as level,
                            description,
                            employer_id_key,
                            province_ids
                        FROM
                            `data-warehouse-sv.derived.job_post_master`
                        WHERE
                            province_ids IS NOT NULL
                    ) mat ON con.job_id_ref = mat.id
                    and con.channel_code = mat.channel_code
            ) l
            LEFT JOIN (
                SELECT
                    DISTINCT org.employer_id_key,
                    org.channel_code,
                    IFNULL(org.tax_code, des.tax_code) AS tax_code
                FROM
                    (
                        SELECT
                            employer_id_key,
                            channel_code,
                            ANY_VALUE(company_hash_id) as company_hash_id,
                            ANY_VALUE(tax_code) as tax_code
                        FROM
                            (
                                SELECT
                                    m.employer_id_key,
                                    m.company_hash_id,
                                    m.channel_code,
                                    IFNULL(m.tax_code, f.tax_code) as tax_code
                                FROM
                                    (
                                        SELECT
                                            employer_id_key,
                                            cast(
                                                FARM_FINGERPRINT(
                                                    COALESCE(
                                                        derived.normalize_tax_code(tax_code),
                                                        derived.normalize_tax_name(name)
                                                    )
                                                ) as string
                                            ) AS company_hash_id,
                                            channel_code,
                                            tax_code
                                        FROM
                                            `data-warehouse-sv.derived.employer_master`
                                    ) m
                                    JOIN (
                                        SELECT
                                            employer_id_key,
                                            cast(company_hash_id as string) as company_hash_id,
                                            channel_code,
                                            IFNULL(effect_tax_code, crm_tax_code) as tax_code
                                        FROM
                                            `data-warehouse-sv.derived.crm_effect_transaction`
                                    ) f ON m.employer_id_key = f.employer_id_key
                            ) mm
                        GROUP BY
                            employer_id_key,
                            channel_code
                    ) org
                    LEFT JOIN `raw_data.persisted_public_mapping_sv_tax` AS des 
                    ON org.company_hash_id = des.company_hash_id
            ) r ON l.employer_id_key = r.employer_id_key
        """).to_dataframe()

        return df_vnw_job, df_cb_job, df_topcv_job, df_raw_sv_job

    @staticmethod
    def reverse_province_ids(x, dct_province_mapping):
        rs = list()
        province_ids = x['province_ids']
        channel_code = x['channel_code']
        if not isinstance(province_ids, str):
            return None
        for _id in province_ids.replace(',', '-').split('-'):
            v = dct_province_mapping.get((channel_code, _id))
            if v is not None:
                rs.append(v)
        return rs

    def transform(self, df_vnw_job, df_cb_job, df_topcv_job, df_raw_sv_job):
        df_province_mapping = pd.read_excel(self.sv_mapping_path, dtype=str)
        dct_province_mapping = dict(df_province_mapping.apply(
            lambda x: ((x['channel_code'], x['original_value']), x['mapping_name']),
            axis=1
        ).tolist())
        df_raw_sv_job['areas'] = df_raw_sv_job[['province_ids', 'channel_code']].apply(
            lambda x: self.reverse_province_ids(x, dct_province_mapping), axis=1)
        df_raw_sv_job['level'] = df_raw_sv_job['level'].map(lambda x: self.map_level.get(x, "Nhân viên"))
        df_raw_sv_job['tax_code'] = df_raw_sv_job['tax_code'].fillna('')
        df_raw_sv_job['name'] = df_raw_sv_job['name'].fillna('')
        df_sv_job = df_raw_sv_job[['id', 'tax_code', 'name', 'company_id', 'level',
                                   'areas', 'description', 'channel_code']].drop_duplicates(['id'])

        df_vnw_job['tax_code'] = df_vnw_job['tax_code'].fillna('')
        df_vnw_job['channel_code'] = 'VNW'
        df_cb_job['tax_code'] = df_cb_job['tax_code'].fillna('')
        df_cb_job['channel_code'] = 'CB'
        df_topcv_job['tax_code'] = df_topcv_job['tax_code'].fillna('')
        df_topcv_job['channel_code'] = 'TOPCV'
        df_cb_job['level'] = df_cb_job['level'].map(lambda x: self.map_level.get(x, x))
        df_topcv_job['level'] = df_topcv_job['level'].map(lambda x: self.map_level.get(x, x))
        df_job = pd.concat([df_vnw_job, df_cb_job, df_topcv_job, df_sv_job], ignore_index=True)

        df_job['norm_name'] = df_job['name'].map(lambda x: x.strip().lower())
        df_job['token_name'] = df_job['norm_name'].map(
            lambda x: token_name(x)
        )
        df_job['global_company_id'] = df_job.apply(lambda x: gen_global_company_id(x), axis=1)

        df_group = df_job.groupby(['global_company_id'])[
            ['id', 'token_name', 'areas', 'level', 'description']].agg(list).reset_index()
        n_jobs = 2
        block_process = 200
        with Parallel(n_jobs=n_jobs) as parallel:
            duplicates = list()
            with tqdm.tqdm(total=df_group.shape[0]) as pbar:
                for i in range(0, df_group.shape[0], block_process):
                    _df = df_group.iloc[i:i+block_process]
                    results = parallel(delayed(build_network_duplicates)(*v) for v in _df.values.tolist())
                    duplicates.extend(results)
                    pbar.update(_df.shape[0])

        duplicates = {
            k: v for item in duplicates for k, v in item.items()}

        df_job['is_duplicate'] = df_job['id'].map(lambda x: x in duplicates)
        df_job['duplicate_id'] = df_job['id'].map(lambda x: duplicates.get(x))

        return df_job[['id', 'channel_code',
                       'is_duplicate', 'duplicate_id']]

    def load(self, df_job_dup):
        insert_df_to_postgres_fast(
            self.postgres_conf, tbl_name="job_duplicates", df=df_job_dup, mode="replace")

    def execute(self):
        self.log.info("step extract")
        df_vnw_job, df_cb_job, df_topcv_job, df_raw_sv_job = self.extract()
        self.log.info("step transform")
        df_job_dup = self.transform(df_vnw_job, df_cb_job, df_topcv_job, df_raw_sv_job)
        self.log.info("step load")
        self.load(df_job_dup)


if __name__ == '__main__':
    import json
    import datetime

    conf = {
        "process_name": "test",
        "execution_date": datetime.datetime.now(),
        "from_date": datetime.datetime(2020, 11, 24),
        "to_date": datetime.datetime(2021, 2, 24),
        "params": {
            "telegram": json.load(open("/Users/thucpk/IdeaProjects/crawlab/etl/config/default/telegram.json")),
            "db": json.load(open("/Users/thucpk/IdeaProjects/crawlab/etl/config/default/db.json")),
            "gcp_sv_data_account_key_path":
                "/Users/thucpk/IdeaProjects/crawlab/etl/config/default/gcp_sv_data_account.json",
            "sv_mapping_path":
                "/Users/thucpk/IdeaProjects/crawlab/etl/config/mapping/[DS] Dimension lookup - SV Owner.xlsx"
        }
    }
    task = TaskCheckJobDuplicates(conf)
    task.execute()
