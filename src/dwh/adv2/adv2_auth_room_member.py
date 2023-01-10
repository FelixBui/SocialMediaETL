from dwh.adv2.adv2_base import Adv2Base


class Adv2AuthRoomMember(Adv2Base):
    def __init__(self, config):
        super(Adv2AuthRoomMember, self).__init__(config)
        self.columns = ['id', 'staff_id', 'room_id', 'channel_code',
                        'created_at', 'updated_at', 'created_by', 'updated_by', 'update_ts']
        self.fix_dts = ['created_at', 'updated_at']
        self.mysql_conf = self.get_param_config(['db', 'adv2_epsilon'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
