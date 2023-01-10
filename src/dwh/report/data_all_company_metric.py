from dwh.report.report_base import ReportBase
from libs.storage_utils import load_df_from_bq, insert_df_to_gsheet, load_sa_creds, load_gsheet_creds
from time import sleep

class DataAllCompanyMetric(ReportBase):
    def __init__(self, config):
        super(DataAllCompanyMetric, self).__init__(config)
        dp_ops = self.get_param_config(['dp-ops'])
        self.gcreds = load_gsheet_creds(dp_ops)
        self.bq_creds = load_sa_creds(dp_ops)
        self.gsheet_id = self.get_param_config(['gsheet_id'])
        self.sheets = self.get_param_config(['sheets'])
        self.report_queries = self.get_param_config(['report_queries'])

    @staticmethod
    def transform(dfs):
        return dfs

    def extract(self):
        dfs = list()
        for ws in self.sheets:
            query = self.report_queries[ws]
            df = load_df_from_bq(self.bq_creds, query)
            dfs.append(df)
        return dfs

    def execute(self):
        dfs = self.extract()
        dfs = self.transform(dfs)
        sheets = self.sheets
        for df, sheet in zip(dfs, sheets):
            insert_df_to_gsheet(self.gcreds, self.gsheet_id, sheet, df)
            sleep(60)