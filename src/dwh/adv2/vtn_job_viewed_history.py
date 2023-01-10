from dwh.adv2.adv2_base import Adv2Base


class VtnJobViewedHistory(Adv2Base):
    def __init__(self, config):
        super(VtnJobViewedHistory, self).__init__(config)
        self.columns = ['job_id', 'seeker_id', 'created_at', 'updated_at', 'deleted']
        self.fix_dts = ['created_at', 'updated_at']

        self.mysql_conf = self.get_param_config(['db', 'vtn'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
