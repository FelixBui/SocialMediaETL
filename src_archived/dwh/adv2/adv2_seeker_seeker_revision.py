from dwh.adv2.adv2_base import Adv2Base


class Adv2SeekerSeekerRevision(Adv2Base):
    def __init__(self, config):
        super(Adv2SeekerSeekerRevision, self).__init__(config)
        self.columns = [
            'id', 'channel_code', 'seeker_id', 'revision_status',
            'seeker_status', 'name', 'email', 'gender', 'marital_status',
            'birthday', 'address', 'mobile', 'province_id', 'avatar',
            'token', 'token_email', 'approved_by', 'approved_at',
            'rejected_reason', 'rejected_reason_note', 'created_at',
            'created_by', 'updated_at', 'updated_by', 'created_source',
            'snap_shot', 'update_ts']
        self.fix_dts = ['birthday', 'approved_at', 'created_at', 'updated_at', 'update_ts']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
