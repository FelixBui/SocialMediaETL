from dwh.report.report_base import ReportBase
from libs.storage_utils import load_df_from_postgres, insert_df_to_gsheet, load_gsheet_creds


class AccDebt(ReportBase):
    def __init__(self, config):
        super(AccDebt, self).__init__(config)
        dp_ops = self.get_param_config(['dp-ops'])
        self.creds = load_gsheet_creds(dp_ops)
        self.gsheet_id = self.get_param_config(['gsheet_id'])
        self.pg_conf = self.get_param_config(['db', 'erp_prod'])
        self.debt_by_aging_query = self.get_param_config(['debt_by_aging_query'])
        self.debt_by_month_query = self.get_param_config(['debt_by_month_query'])
        self.sheets = self.get_param_config(['sheets'])

    def extract(self):
        debt_by_aging_df = load_df_from_postgres(self.pg_conf, query=self.debt_by_aging_query)
        debt_by_month_df = load_df_from_postgres(self.pg_conf, query=self.debt_by_month_query)
        return debt_by_aging_df, debt_by_month_df

    def execute(self):
        dfs = self.extract()
        sheets = self.sheets
        for df, sheet in zip(dfs, sheets):
            insert_df_to_gsheet(self.creds, self.gsheet_id, sheet, df)
