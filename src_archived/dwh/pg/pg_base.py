import re
from abc import ABC

from google.cloud import bigquery
import io
import pandas as pd
import tqdm

from core.app_base import AppBase
from libs.storage_utils import get_gcs_cli, get_bq_cli, get_postgres_engine


class PgBase(AppBase, ABC):
    def __init__(self, config):
        super(PgBase, self).__init__(config)
        dp_ops = self.get_param_config(['dp-ops'])
        self.gcs_cli = get_gcs_cli(dp_ops)
        self.bq_cli = get_bq_cli(dp_ops)
        self.bucket_name = self.get_param_config(['bucket_name'])
        self.db_name = self.get_param_config(["db_name"])
        self.tbl_name = self.get_param_config(["tbl_name"])
        self.bq_tbl = self.get_param_config(['bq_tbl'])
        # self.schema_name = self.get_param_config(['schema_name'])
        self.pg_conf = dict()
        self.columns = list()
        self.fix_dts = list()
        self.cast_float = list()
        self.cast_dt = list()
        self.column_seed = ''
        self.time_col = ''
        self.gcs_path_template = "{0}/{1}/{2}/{1}_{{}}.pq".format(
            self.db_name, self.tbl_name,
            str(self.execution_date.date())
        )

    def transform(self, df):
        for col in self.fix_dts:
            df[col] = df[col].astype('str')
        df = df.convert_dtypes()
        for col in self.cast_float:
            df[col] = df[col].astype('float')
        for col in self.cast_dt:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        return df

    def mapping_bq_type(self, x):
        pattern = re.compile(r'ARRAY|anyarray|character|date|double|integer|timestamp|text|boolean|numeric')
        map_value = {
            'ARRAY': 'string',
            'anyarray': 'string',
            'boolean': 'float',
            'character': 'string',
            'date': 'string',
            'double': 'float',
            'integer': 'integer',
            'numeric': 'integer',
            'text': 'string',
            'timestamp': 'timestamp'
        }
        if x[0] in self.fix_dts:
            return bigquery.SchemaField(x[0], "string")
        elif x[0] in self.cast_float:
            return bigquery.SchemaField(x[0], "float")
        else:
            return bigquery.SchemaField(
                x[0],
                map_value.get(
                    pattern.search(x[1]).group(),
                    'string')
            )

    def fetch_schema(self):
        pg_engine = get_postgres_engine(self.pg_conf)
        q = """
        SELECT column_name, data_type 
        FROM information_schema.columns
        WHERE table_name = '{0}' AND table_schema = '{1}'
        """.format(self.tbl_name, self.db_name)
        pg_schema = pg_engine.execute(q).fetchall()
        pg_schema = [x for x in pg_schema if x[0] in [col.replace('"', '') for col in self.columns]]
        # for ix, x in enumerate(pg_schema):
        #     if pattern.search(x[1]).group() == 'blob':
        #         self.columns.pop(ix)
        #         pg_schema.pop(ix)

        bq_schema = [self.mapping_bq_type(x) for x in pg_schema]
        return bq_schema

    def load_gcs(self, sub_df, gcs_path):
        iob = io.BytesIO()
        sub_df.to_parquet(iob, engine="pyarrow")
        chuck_size = 1024 * 256
        bucket = self.gcs_cli.get_bucket(self.bucket_name)
        blob = bucket.blob(gcs_path, chunk_size=chuck_size)
        iob.seek(0)
        blob.upload_from_file(iob)

    def load_bq(self, bq_tbl, gcs_uri):
        self.bq_cli.delete_table(self.bq_tbl, not_found_ok=True)
        schema = self.fetch_schema()
        table = bigquery.Table(self.bq_tbl, schema=schema)
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
        pg_engine = get_postgres_engine(self.pg_conf)
        q = f"SELECT COUNT(1) FROM {self.db_name}.{self.tbl_name}"
        num_rows = pg_engine.execute(q).fetchone()[0]
        i = 0
        step_interval = 200000
        seed_id = 0
        for id_from in tqdm.tqdm(range(0, num_rows, step_interval)):
            id_to = min(step_interval, num_rows + 1 - id_from)
            print(id_from, id_to)
            q = f"SELECT {','.join(self.columns)} " \
                f"FROM {self.db_name}.{self.tbl_name} " \
                f"WHERE {self.column_seed} > {seed_id} " \
                f"ORDER BY {self.column_seed} " \
                f"LIMIT {id_to}"
            raw_df = pd.read_sql(q, pg_engine)
            df = self.transform(raw_df)
            seed_id = df[self.column_seed].max()
            gcs_path = self.gcs_path_template.format(i)
            self.load_gcs(df, gcs_path)
            i += 1
        self.load_bq(self.bq_tbl, "gs://{}/{}".format(self.bucket_name, self.gcs_path_template.format("*")))

    def upsert(self):
        mysql_conn = get_postgres_engine(self.pg_conf)
        q = f"SELECT count(1) FROM {self.db_name}.{self.tbl_name} " \
            f"WHERE {self.time_col} >= '{self.from_date}' AND {self.time_col} < '{self.to_date}' "
        num_rows = mysql_conn.execute(q).fetchone()[0]
        i = 0
        step_interval = 200000
        for id_from in tqdm.tqdm(range(0, num_rows, step_interval)):
            id_to = min(step_interval, num_rows + 1 - id_from)
            # print(id_from, id_to)
            q = f"SELECT {', '.join(self.columns)} " \
                f"FROM {self.db_name}.{self.tbl_name} " \
                f"WHERE {self.time_col} >= '{self.from_date}' AND {self.time_col} < '{self.to_date}' " \
                f"ORDER BY {self.column_seed} " \
                f"LIMIT {id_from}, {id_to}"
            raw_df = pd.read_sql(q, mysql_conn)
            df = self.transform(raw_df)
            gcs_path = self.gcs_path_template.format(i)
            self.load_gcs(df, gcs_path)
            i += 1

        self.load_bq(self.bq_tbl, "gs://{}/{}".format(self.bucket_name, self.gcs_path_template.format("*")))
