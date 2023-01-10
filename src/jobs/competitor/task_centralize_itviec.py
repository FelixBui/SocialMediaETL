from core.app_base import AppBase
import pandas as pd
from dateutil import tz

from libs.storage_utils import load_df_from_mongo, insert_df_to_postgres, load_df_from_postgres

local_tz = tz.tzlocal()


class TaskCentralizeItviec(AppBase):
    job_cols = [
        '_id', 'id', 'objectID', 'name', 'company_id',  'class_job',  'url', 'tag_lists',
        'areas', 'salary', 'sourcetype', 'address', 'benefits', 'description',
        'requirement', 'why_love', 'work_time', 'region', 'ot',
        'job_updated_at', 'created_at', 'updated_at']

    company_cols = [
        'company_id', 'company_name', 'company_summary',  'areas',
        'address', 'company_url', 'company_type', 'company_size',
        'created_at', 'updated_at']

    mapping_company = {
        'company_id': 'id',
        'company_name': 'name',
        'company_summary': 'summary',
        'company_url': 'url',
        'company_size': 'size'
    }

    map_size_name = {
        0: "Không xác định",
        1: "Dưới 20 người",
        2: "20 - 150 người",
        3: "150 - 300 người",
        4: "Trên 300 người"
    }

    def __init__(self, config):
        super(TaskCentralizeItviec, self).__init__(config)
        self.mongo_conf = self.get_param_config(['db', 'mg-competitor'])
        self.postgres_conf = self.get_param_config(['db', 'pg-competitor'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

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

    def extract(self):
        query = {
            "updated_at": {
                "$gte": self.from_date,
                "$lt": self.to_date
            }
        }
        df_raw = load_df_from_mongo(self.mongo_conf, collection_name="itviec", query=query, no_id=False)
        df_raw['created_at'] = df_raw['_id'].map(lambda x: x.generation_time.astimezone(local_tz))

        return df_raw

    def transform(self, df_raw):
        df_raw['tag_lists'] = df_raw['tag_lists'].map(
            lambda x: [e.strip() for e in x] if isinstance(x, list) else x)
        df_raw['description'] = df_raw['description'].map(
            lambda x: "\n".join([e.strip() for e in x if e.strip() != '']) if isinstance(x, list) else x)
        df_raw['requirement'] = df_raw['requirement'].map(
            lambda x: "\n".join([e.strip() for e in x if e.strip() != '']) if isinstance(x, list) else x)
        df_raw['why_love'] = df_raw['why_love'].map(
            lambda x: "\n".join([e.strip() for e in x if e.strip() != '']) if isinstance(x, list) else x)
        df_raw['work_time'] = df_raw['work_time'].map(lambda x: x.strip() if isinstance(x, str) else x)
        df_raw['region'] = df_raw['region'].map(lambda x: x.strip() if isinstance(x, str) else x)
        df_raw['ot'] = df_raw['ot'].map(lambda x: x.strip() if isinstance(x, str) else x)

        df_raw['company_id'] = df_raw['company_id'].astype(str)
        df_raw['id'] = df_raw['id'].astype(str)

        for col in self.job_cols:
            if col not in df_raw.columns:
                df_raw[col] = None
        for col in self.company_cols:
            if col not in df_raw.columns:
                df_raw[col] = None

        df_job = df_raw[self.job_cols]
        company_df = df_raw[self.company_cols].rename(columns=self.mapping_company)
        company_ids = company_df['id'].map(lambda x: "'{}'".format(x)).tolist()
        q = """select id from itv_company where id in ({})""".format(",".join(company_ids))
        tmp_df = load_df_from_postgres(self.postgres_conf, q)
        if tmp_df.shape[0] > 0:
            print(company_df.shape)
            company_df = company_df.loc[
                ~company_df['id'].isin(tmp_df['id'])
            ].reset_index(drop=True)
            print(company_df.shape)

        if company_df.shape[0] > 0:
            # norm size
            company_df['size'] = company_df['size'].map(lambda x: x.replace('+', '-999999') if isinstance(x, str) else None)
            company_df['size_min'] = company_df['size'].map(lambda x:  int(x.split('-')[0]) if isinstance(x, str) else None)
            company_df['size_max'] = company_df['size'].map(lambda x:  int(x.split('-')[1]) if isinstance(x, str) else None)
            company_df['size_code'] = company_df['size_max'].map(self.norm_size)
            company_df['size_name'] = company_df['size_code'].map(self.map_size_name)

            # name
            company_df['name'] = company_df['name'].str.strip().str.lower()
            company_df['channel_code'] = 'ITV'

            # tax code
            company_taxid_df = load_df_from_mongo(
                self.mongo_conf, 'company_lookup_result',
                query={"search_rank": 1}, selected_keys=["original_name", "taxid"]).rename(
                columns={"taxid": "tax_code"})
            company_taxid_df['tax_code'] = company_taxid_df['tax_code'].map(lambda x: x.split('-')[0])

            company_df = company_df.join(company_taxid_df.set_index('original_name'), 'name')
            # areas
            # df_company['areas'] = df_company['areas'].map(norm_areas)

        return df_job, company_df

    def load(self, job_df, company_df):
        job_df['_id'] = job_df['_id'].map(str)
        insert_df_to_postgres(self.postgres_conf, tbl_name="itv_job",
                              df=job_df, primary_keys=['id'])
        insert_df_to_postgres(self.postgres_conf, tbl_name="itv_company",
                              df=company_df.drop_duplicates(['id']), primary_keys=['id'])

    def execute(self):
        self.log.info("step extract")
        raw_df = self.extract()
        self.log.info("step transform")
        job_df, company_df = self.transform(raw_df)
        self.log.info("step load")
        self.load(job_df, company_df)
