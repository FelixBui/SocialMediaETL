from dwh.adv2.adv2_base import Adv2Base


class VtnEmailClick(Adv2Base):
    def __init__(self, config):
        super(VtnEmailClick, self).__init__(config)
        self.columns = ['id', 'seeker_id', 'campagin_id', 'created_at']
        self.fix_dts = ['created_at', ]

        self.mysql_conf = self.get_param_config(['db', 'vtn'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
