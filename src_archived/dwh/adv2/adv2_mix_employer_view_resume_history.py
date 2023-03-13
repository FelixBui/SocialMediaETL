from dwh.adv2.adv2_base import Adv2Base


class Adv2MixEmployerViewResumeHistory(Adv2Base):
    def __init__(self, config):
        super(Adv2MixEmployerViewResumeHistory, self).__init__(config)
        self.columns = [
            'id', 'channel_code', 'registration_filter_resume_id', 'employer_id', 'resume_id',
            'seeker_id', 'cache_seeker_name', 'created_source', 'service_code', 'point',
            'ip_address', 'cache_resume_title', 'resume_title_changed', 'created_at', 'created_by',
            'updated_at', 'updated_by', 'update_ts']
        self.fix_dts = ['update_ts']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
