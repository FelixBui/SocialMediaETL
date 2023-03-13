from dwh.adv2.adv2_base import Adv2Base


class Adv2AuthRoom(Adv2Base):
    def __init__(self, config):
        super(Adv2AuthRoom, self).__init__(config)
        self.columns = [
            'id', 'channel_code', 'branch_code', 'code', 'name', 'type', 'status',
            'created_by', 'created_at', 'updated_by', 'updated_at',
            'created_source', 'update_ts']
        self.fix_dts = ['created_at', 'updated_at', 'update_ts']
        self.mysql_conf = self.get_param_config(['db', 'adv2_epsilon'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
