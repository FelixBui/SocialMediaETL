from dwh.adv2.adv2_base import Adv2Base


class Adv2MixAsFilterResumeCampaignHistory(Adv2Base):
    def __init__(self, config):
        super(Adv2MixAsFilterResumeCampaignHistory, self).__init__(config)
        self.columns = [
            'id', 'channel_code', 'employer_id', 'campaign_id', 'resume_id', 'seeker_id', 'is_sent',
            'status', 'is_seen', 'created_at', 'created_by', 'updated_at', 'updated_by', 'update_ts']
        self.fix_dts = ['created_at', 'updated_at', 'update_ts']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
