import re
import pymysql.cursors
from sqlalchemy import create_engine

from core.app_base import AppBase

pymysql.install_as_MySQLdb()


class GenAdv2Etl(AppBase):
    def __init__(self, config):
        super(GenAdv2Etl, self).__init__(config)
        self.mysql_conf = self.get_param_config(["mysql_conf"])
        self.db_name = self.get_param_config(["db_name"])
        self.tbl_name = self.get_param_config(["tbl_name"])
        self.columns = list()
        self.fix_dts = list()
        self.cast_float = list()

    def execute(self):
        mysql_conn = create_engine(
            f'mysql+mysqldb://{self.mysql_conf["user"]}:{self.mysql_conf["password"]}@'
            f'{self.mysql_conf["host"]}:{self.mysql_conf["port"]}/{self.db_name}',
            echo=False, pool_recycle=7200)
        q = f"show columns from {self.tbl_name}"
        mysql_schema = mysql_conn.execute(q).fetchall()
        self.columns = [x[0] for x in mysql_schema]
        self.fix_dts = [x[0] for x in mysql_schema if x[1] in ('datetime', 'date')]
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
            "float": "float",
            "char": "string",
            "time": "string",
            "enum": "string",
            "blob": "string"
        }
        for ix, x in enumerate(mysql_schema):
            print(ix, x[1])
            if pattern.search(x[1]).group() == 'blob':
                self.columns.pop(ix)
                mysql_schema.pop(ix)

        self.cast_float = [x[0] for x in mysql_schema if map_value.get(
            pattern.search(x[1]).group(),
            'string'
        ) == 'float']
        print(self.db_name, self.tbl_name)
        print(mysql_schema)
        print(self.columns)
        print(self.fix_dts)
        print(self.cast_float)
