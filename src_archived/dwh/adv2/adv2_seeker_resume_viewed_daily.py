from dwh.adv2.adv2_base import Adv2Base


class Adv2SeekerResumeViewedDaily(Adv2Base):
    def __init__(self, config):
        super(Adv2SeekerResumeViewedDaily, self).__init__(config)
        self.columns = ['id', 'channel_code', 'resume_id', 'employer_id', 'total',
                        'updated_at', 'created_at', 'update_ts']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.cast_dt = ['updated_at', 'created_at', 'update_ts']
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
