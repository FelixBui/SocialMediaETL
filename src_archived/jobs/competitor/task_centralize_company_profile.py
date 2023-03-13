import tqdm
import pandas as pd
from bson.objectid import ObjectId
from dwh.mg.mg_base import MgBase
import io
from google.cloud import bigquery
from libs.storage_utils import get_gcs_cli, get_bq_cli
import datetime


class TaskCentralizeCompanyProfile(MgBase):

    def __init__(self, config):
        super(TaskCentralizeCompanyProfile, self).__init__(config)
        self.query_template = self.get_param_config(['query_template'])
        self.bq_tbl = self.get_param_config(['bq_tbl'])
        self.col_map = {
            "url": 'url',
            "company_name": 'company_name',
            "id": 'id',
            "objectID": 'object_id',
            "sourcetype": 'source_type',
            "tax_code": 'tax_code',
            "tax_address": 'address',
            "name": 'name',
            "short_name": 'short_name',
            "legal_representative": 'legal_representative',
            "phone": 'phone',
            "tax_created_at": 'tax_created_at',
            "status": "status",
            "created_at": "created_at",
            "primary_industry_id": 'primary_industry_id',
            "primary_industry": 'primary_industry',
            "refreshed_at": 'refreshed_at',
            "business_group_ids": "business_group_ids",
            "business_group_names": "business_group_names",
            "email": "email",
        }
        self.bucket_name = self.get_param_config(['bucket_name'])
        self.gcs_cli = get_gcs_cli(self.dp_ops)
        self.bq_cli = get_bq_cli(self.dp_ops)
        self.gcs_path_template = "competitor/company_profile/{0}/company_profile_{{}}.pq".format(
            str(self.execution_date.date())
        )

    def query(self):
        query = self.query_template
        # from_id = ObjectId.from_datetime(self.from_date)
        # to_id = ObjectId.from_datetime(self.to_date)
        from_id = ObjectId.from_datetime( datetime.datetime(2020, 1, 1))
        to_id = ObjectId.from_datetime( datetime.datetime(2023, 1, 1))
        query.update({"_id": {"$gte": from_id, "$lt": to_id}})
        return query

    def load(self, **kwargs):
        pass

    def load_gcs(self, sub_df, gcs_path):
        iob = io.BytesIO()
        sub_df.to_parquet(iob, engine="pyarrow")
        chuck_size = 1024 * 256
        bucket = self.gcs_cli.get_bucket(self.bucket_name)
        blob = bucket.blob(gcs_path, chunk_size=chuck_size)
        iob.seek(0)
        blob.upload_from_file(iob)

    def load_bq(self, bq_tbl, gcs_uri):
        schema = [
            bigquery.SchemaField("url", "string"),
            bigquery.SchemaField("company_name", "string"),
            bigquery.SchemaField("id", "string"),
            bigquery.SchemaField("object_id", "string"),
            bigquery.SchemaField("source_type", "string"),
            bigquery.SchemaField("tax_code", "string"),
            bigquery.SchemaField("address", "string"),
            bigquery.SchemaField("name", "string"),
            bigquery.SchemaField("short_name", "string"),
            bigquery.SchemaField("legal_representative", "string"),
            bigquery.SchemaField("phone", "string"),
            bigquery.SchemaField("tax_created_at", "string"),
            bigquery.SchemaField("primary_industry", 'string'),
            bigquery.SchemaField("primary_industry_id", "string"),
            bigquery.SchemaField("status", "string"),
            bigquery.SchemaField("created_at", "timestamp"),
            # bigquery.SchemaField("updated_at", "timestamp"),
            bigquery.SchemaField("refreshed_at", "timestamp"),
            bigquery.SchemaField("business_group_ids", "string"),
            bigquery.SchemaField("business_group_names", "string"),
            bigquery.SchemaField("email", "string")
        ]
        self.bq_cli.delete_table(self.bq_tbl, not_found_ok=True)
        # schema = self.fetch_schema()
        table = bigquery.Table(
            self.bq_tbl, schema=schema)
        self.bq_cli.create_table(table)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema=schema
        )

        load_job = self.bq_cli.load_table_from_uri(
            gcs_uri, bq_tbl, job_config=job_config
        )
        load_job.result()
        bq_rs = self.bq_tbl.replace("test", "src_raw")
        load_job = self.bq_cli.query(
            "CREATE OR REPLACE TABLE {} AS (SELECT * FROM {})".format(bq_rs, self.bq_tbl)
        )
        load_job.result()

    def full(self):
        q = self.query()
        num_rows = self.src_db[self.col].count_documents(q)
        col = self.src_db[self.col].find(
            q, self.col_map.keys()
        )
        i = 0
        step_interval = 200000
        lst = list()
        for e in tqdm.tqdm(col, total=num_rows):
            lst.append(e)
            if len(lst) % step_interval == 0:
                raw_df = pd.DataFrame(lst)
                df = self.transform(raw_df)
                gcs_path = self.gcs_path_template.format(i)
                self.load_gcs(df, gcs_path)
                lst = list()
                i += 1
        if len(lst) > 0:
            raw_df = pd.DataFrame(lst)
            df = self.transform(raw_df)
            gcs_path = self.gcs_path_template.format(i)
            self.load_gcs(df, gcs_path)
        self.load_bq(
            self.bq_tbl, "gs://{}/{}".format(self.bucket_name, self.gcs_path_template.format("*")))

    def execute(self):
        self.full()

    def transform(self, raw_df):
        df = raw_df.rename(columns=self.col_map)
        df['created_at'] = df['_id'].map(lambda x: x.generation_time)
        df = df.drop(columns=['_id'])
        df = df.rename(columns=self.col_map)
        df['business_group_ids'] = df['business_group_ids'].map(
            lambda x: ', '.join(x) if isinstance(x, list) else x)
        df['business_group_names'] = df['business_group_names'].map(
            lambda x: ', '.join(x) if isinstance(x, list) else x)
        
        df['id'] = df['id'].astype(str)
        df['url'] = df['url'].astype(str)
        df['company_name'] = df['company_name'].astype(str)
        df['object_id'] = df['object_id'].astype(str)
        df['source_type'] = df['source_type'].astype(str)
        df['tax_code'] = df['tax_code'].astype(str)
        if 'tax_address' in df.columns:
            df['tax_address'] = df['tax_address'].astype(str)
        df['name'] = df['name'].astype(str)
        df['short_name'] = df['short_name'].astype(str)
        df['legal_representative'] = df['legal_representative'].astype(str)
        df['phone'] = df['phone'].astype(str)
        df['tax_created_at'] = df['tax_created_at'].astype(str)
        df['primary_industry'] = df['primary_industry'].astype(str)
        df['primary_industry_id'] = df['primary_industry_id'].astype(str)
        # df['created_at'] = df['created_at'].astype(str)
        # df['refreshed_at'] = df['refreshed_at'].astype(str)
        df['business_group_ids'] = df['business_group_ids'].astype(str)
        df['business_group_names'] = df['business_group_names'].astype(str)
        df['email'] = df['email'].astype(str)
        df['refreshed_at'] = pd.to_datetime(df['refreshed_at'],errors='coerce')
        return df.fillna('')
