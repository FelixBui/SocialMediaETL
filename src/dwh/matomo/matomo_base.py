import re
from abc import ABC
import base64
from google.cloud import bigquery
import pymysql.cursors
import io
import pandas as pd
from sqlalchemy import create_engine
import tqdm

from core.app_base import AppBase
from libs.storage_utils import get_gcs_cli, get_bq_cli

pymysql.install_as_MySQLdb()


class MatomoBase(AppBase, ABC):
    def __init__(self, config):
        super(MatomoBase, self).__init__(config)
        dp_ops = self.get_param_config(['dp-ops'])
        self.gcs_cli = get_gcs_cli(dp_ops)
        self.bq_cli = get_bq_cli(dp_ops)
        self.bucket_name = self.get_param_config(['bucket_name'])
        self.db_name = self.get_param_config(["db_name"])
        self.tbl_name = self.get_param_config(["tbl_name"])
        self.bq_tbl = self.get_param_config(['bq_tbl'])
        self.mysql_conf = dict()
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
        df = df.applymap(lambda x: base64.b64encode(x).decode('utf-8') if isinstance(x, bytes) else x)

        return df

    def fetch_schema(self):
        mysql_conn = create_engine(
            f'mysql+mysqldb://{self.mysql_conf["user"]}:{self.mysql_conf["password"]}@'
            f'{self.mysql_conf["host"]}:{self.mysql_conf["port"]}/{self.db_name}',
            echo=False, pool_recycle=7200)
        q = "show columns from {}".format(self.tbl_name)
        mysql_schema = mysql_conn.execute(q).fetchall()
        mysql_schema = [x for x in mysql_schema if x[0] in self.columns]
        pattern = re.compile(r'(datetime)|(decimal)|(int)|(text)|(varchar)|(date)|(binary)|(double)|(char)'
                             r'|(time)|(blob)|(enum)|(float)')
        map_value = {
            "datetime": "timestamp",
            "decimal": "float",
            "int": "integer",
            "text": "string",
            "varchar": "string",
            "date": "timestamp",
            "binary": "string",
            "double": "float",
            "char": "string",
            "time": "string",
            "enum": "string",
            "blob": "string",
            "float": "float"
        }
        for ix, x in enumerate(mysql_schema):
            if pattern.search(x[1]).group() == 'blob':
                self.columns.pop(ix)
                mysql_schema.pop(ix)

        bq_schema = [bigquery.SchemaField(
            x[0],
            map_value.get(
                pattern.search(x[1]).group(),
                'string'
            ) if x[0] not in self.fix_dts else 'string')
            for x in mysql_schema]
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
        mysql_conn = create_engine(
            f'mysql+mysqldb://{self.mysql_conf["user"]}:{self.mysql_conf["password"]}@'
            f'{self.mysql_conf["host"]}:{self.mysql_conf["port"]}/{self.db_name}',
            echo=False, pool_recycle=7200)

        # q = f"SELECT table_rows " \
        #     f"FROM INFORMATION_SCHEMA.TABLES " \
        #     f"WHERE TABLE_SCHEMA = 'matomo' " \
        #     f"AND table_name = '{self.tbl_name}' "
        q = f"SELECT `AUTO_INCREMENT` " \
            f"FROM  INFORMATION_SCHEMA.TABLES " \
            f"WHERE TABLE_SCHEMA = 'matomo' " \
            f"AND   TABLE_NAME   = '{self.tbl_name}' "
        num_rows = mysql_conn.execute(q).fetchone()[0]
        i = 0
        step_interval = 100000
        seed_id = 0
        for id_from in tqdm.tqdm(range(0, num_rows, step_interval)):
            id_to = min(step_interval, num_rows + 1 - id_from)
            next_offset = seed_id + id_to
            print(id_from, next_offset)
            q = f"SELECT {','.join(self.columns)} " \
                f"FROM {self.tbl_name} " \
                f"WHERE {self.column_seed} > {seed_id} and {self.column_seed} <= {next_offset} " \
                f"ORDER BY {self.column_seed} " \
                f"LIMIT {id_to}"
            raw_df = pd.read_sql(q, mysql_conn)
            df = self.transform(raw_df)
            seed_id = df[self.column_seed].max()
            gcs_path = self.gcs_path_template.format(i)
            self.load_gcs(df, gcs_path)
            i += 1
        self.load_bq(self.bq_tbl, "gs://{}/{}".format(self.bucket_name, self.gcs_path_template.format("*")))

    def execute(self):
        self.full()
