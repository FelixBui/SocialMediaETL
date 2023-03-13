from dwh.adv2.adv2_base import Adv2Base


class Adv2MixEmployerSeenResume(Adv2Base):
    def __init__(self, config):
        super(Adv2MixEmployerSeenResume, self).__init__(config)
        self.columns = ['id', 'channel_code', 'resume_id', 'employer_id', 'created_at', 'update_ts']
        self.fix_dts = ['update_ts']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
