import os
import cx_Oracle
import csv
import logging
from pathlib import Path
from airflow.hooks.oracle_hook import OracleHook

class Oracle2Csv:
    # Fixing UTF8 issues
    os.environ["NLS_LANG"] = "American_America.UTF8"

    def __init__(self,
                csv_file_name,
                data_path,
                sql_query,
                conn_id,
                split_mode=True,
                split_nrows=500000,
                array_size=1000,
                header_case="upper",
                ):
        self.conn_id = conn_id
        self.sql_query = sql_query
        self.csv_file_name = csv_file_name
        self.data_path = data_path
        self.split_mode = split_mode
        self.split_nrows = split_nrows
        self.array_size = array_size
        self.header_case = header_case
        self.output_files = []

    def execute(self):
        def _date2string(value):
            return str(value)

        def _output_handler(cursor, name, defaulttype, length, precision, scale):
            if defaulttype == cx_Oracle.DATETIME:
                return cursor.var(cx_Oracle.STRING, arraysize=cursor.arraysize, outconverter=_date2string)
        
        # Create directory if not exists
        Path(self.data_path).mkdir(parents=True, exist_ok=True)
        output_csv_file_path = self.generate_file_name(self.data_path, self.csv_file_name, self.split_mode)
        
        # Get columns name
        column_names = self.get_columns(self.sql_query)
        
        # Initialize first csv file
        part = 1
        total_rows = 0
        _total_rows = 0
        self.create_csv(column_names, output_csv_file_path)

        csv_file_conn = open(output_csv_file_path, mode="a", encoding="utf-8")
        csv_writer_row = csv.writer(csv_file_conn, dialect=csv.excel, delimiter=",", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

        # with cx_Oracle.connect(oracle_db_user,oracle_db_pass,oracle_db_dns) as con:
        with OracleHook(oracle_conn_id=self.conn_id).get_conn() as con:
            # Use in case to handle weird data need to convert before feeding to python object
            con.outputtypehandler = _output_handler
            with con.cursor() as cur:
                # Ensure the datetime querying from Oracle is in the desired format
                # cur.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'")
                cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")

                # Start execute the query with defined array_size
                # Noted that array size contains number of return record stored in RAM in every fetching command
                cur.arraysize = self.array_size
                cur.execute(self.sql_query)
                while True:
                    rows = cur.fetchmany()
                    if not rows:
                        logging.info(f"Query: {self.sql_query} returns {_total_rows} rows.")
                        break

                    if self.split_mode:
                        # Write file in split_mode
                        if total_rows < self.split_nrows:
                            csv_writer_row.writerows(rows)
                        else:
                            # Close file connections
                            csv_file_conn.close()

                            # Create new csv file
                            part += 1
                            output_csv_file_path = self.generate_file_name(self.data_path, self.csv_file_name, self.split_mode, part)

                            self.create_csv(column_names, output_csv_file_path)

                            # Append new rows to existing csv file
                            csv_file_conn = open(output_csv_file_path, mode="a", encoding="utf-8")
                            csv_writer_row = csv.writer(csv_file_conn, dialect=csv.excel, delimiter=",", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
                            csv_writer_row.writerows(rows)

                            # Reset total rows
                            total_rows = 0
                    else:
                        # Write to a single file
                        csv_writer_row.writerows(rows)

                    total_rows += len(rows)
                    _total_rows += len(rows)

        csv_file_conn.close()

        # Rename file name if only one file available in split mode
        if len(self.output_files) == 1:
            old_output_file = self.output_files[0]
            new_output_file = old_output_file[:old_output_file.find("-part")] + ".csv"
            os.replace(old_output_file,new_output_file) # overwrite if existing same file name
            return [new_output_file]

        return self.output_files

    def generate_file_name(self, path, file_name, multi_part, part=1):
        if not multi_part:
            output_csv_file_path = os.path.join(path, file_name)
        else:
            output_csv_file_path = os.path.join(path, file_name[:-4] + "-part" + str(part) + ".csv")
            self.output_files.append(output_csv_file_path)

        return output_csv_file_path

    def get_columns(self, query):
        # with cx_Oracle.connect(oracle_db_user,oracle_db_pass,oracle_db_dns) as con:
        with OracleHook(oracle_conn_id=self.conn_id).get_conn() as con:
            with con.cursor() as cur:
                cur.arraysize = 1
                cur.execute(query)
                # Get column name from cursor & write column name to csv file
                column_names = None
                if self.header_case == "lower":
                    column_names = [col[0].lower() for col in cur.description]
                elif self.header_case == "upper":
                    column_names = [col[0].upper() for col in cur.description]
                else:
                    column_names = [col[0] for col in cur.description]

                return column_names

    def create_csv(self, column_names, output_csv_file_path):
        csv_file_conn = open(output_csv_file_path, mode = "w", encoding="utf-8")
        csv_writer_column = csv.writer(csv_file_conn, dialect=csv.excel, delimiter=",", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        csv_writer_column.writerows([column_names])
        csv_file_conn.close()

if __name__=="__main__":
    oracle_db_user = "AP_BI"
    oracle_db_pass = "__pass__"
    oracle_db_dns = "DVNCL01-PROD.VN.PROD/HDWVN.HOMECREDIT.VN"

    csv_file_name = "R0699_DAILY_ACC_MOVE_DETAILS.csv"
    data_path = "/tmp"
    sql_query = "SELECT * FROM AP_FIN_BI.R0699_DAILY_ACC_MOVE_DETAILS"
    orc_extract = Oracle2Csv(csv_file_name, data_path, sql_query)
    orc_extract.execute()