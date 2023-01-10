from dwh.adv2.adv2_base import Adv2Base


class Adv2MixAsFilterResumeCampaign(Adv2Base):
    def __init__(self, config):
        super(Adv2MixAsFilterResumeCampaign, self).__init__(config)
        self.columns = [
            'id', 'channel_code', 'employer_id', 'job_id', 'name', 'quantity_cv',
            'content', 'status', 'expired_at', 'approved_at', 'type', 'reason', 'created_at',
            'created_by', 'updated_at', 'updated_by', 'update_ts']
        self.fix_dts = ['expired_at', 'approved_at', 'created_at', 'updated_at', 'update_ts']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
