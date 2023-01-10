import re

from core.app_base import AppBase
from libs.storage_utils import get_postgres_engine


class GenPgEtl(AppBase):
    def __init__(self, config):
        super(GenPgEtl, self).__init__(config)
        self.postgres_conf = self.get_param_config(["postgres_conf"])
        self.db_name = self.get_param_config(["db_name"])
        self.tbl_name = self.get_param_config(["tbl_name"])
        self.columns = list()
        self.fix_dts = list()
        self.cast_float = list()

    def execute(self):
        pg_engine = get_postgres_engine(self.postgres_conf)
        q = """
        SELECT column_name, data_type 
        FROM information_schema.columns
        WHERE table_name = '{0}' AND table_schema = '{1}'
        """.format(self.tbl_name, self.db_name)
        pg_schema = pg_engine.execute(q).fetchall()
        self.columns = [x[0] for x in pg_schema]
        dt_pattern = re.compile(r'timestamp|date')
        self.fix_dts = [x[0] for x in pg_schema if dt_pattern.search(x[1])]
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
        # for ix, x in enumerate(pg_schema):
        #     if pattern.search(x[1]).group() == 'blob':
        #         self.columns.pop(ix)
        #         pg_schema.pop(ix)

        self.cast_float = [x[0] for x in pg_schema if map_value.get(
            pattern.search(x[1]).group(),
            'string'
        ) == 'float']
        print(self.db_name, self.tbl_name)
        print(pg_schema)
        print(self.columns)
        print(self.fix_dts)
        print(self.cast_float)
