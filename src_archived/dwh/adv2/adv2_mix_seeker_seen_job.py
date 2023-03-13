from dwh.adv2.adv2_base import Adv2Base


class Adv2MixSeekerSeenJob(Adv2Base):
    def __init__(self, config):
        super(Adv2MixSeekerSeenJob, self).__init__(config)
        self.columns = ['id', 'channel_code', 'seeker_id', 'job_id',
                        'created_at', 'updated_at', 'update_ts']
        self.cast_dt = ['created_at', 'updated_at', 'update_ts']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
