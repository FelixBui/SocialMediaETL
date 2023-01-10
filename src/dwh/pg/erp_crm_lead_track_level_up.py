from dwh.pg.pg_base import PgBase


class ErpCrmLeadTrackLevelUp(PgBase):
    def __init__(self, config):
        super(ErpCrmLeadTrackLevelUp, self).__init__(config)
        self.columns = [
            'id', 'stage_id', 'tracking_date', 'user_id', 'lead_id',
            'create_uid', 'create_date', 'write_uid', 'write_date']
        self.fix_dts = ['tracking_date', 'create_date', 'write_date']
        self.pg_conf = self.get_param_config(['db', 'erp-prod'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
