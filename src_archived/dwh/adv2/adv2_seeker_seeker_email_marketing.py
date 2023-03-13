from dwh.adv2.adv2_base import Adv2Base


class Adv2SeekerSeekerEmailMarketing(Adv2Base):
    def __init__(self, config):
        super(Adv2SeekerSeekerEmailMarketing, self).__init__(config)
        self.columns = [
            'id', 'channel_code', 'seeker_id', 'resume_id', 'full_name', 'frequency', 'gender',
            'fields', 'provinces', 'level', 'position', 'experience', 'salary', 'work_time',
            'last_sent', 'next_run', 'total_sent', 'job_sent', 'created_type', 'work_expired_at',
            'status', 'created_at', 'created_by', 'updated_at', 'updated_by', 'created_source', 'update_ts']
        self.fix_dts = ['last_sent', 'next_run', 'work_expired_at']  # 'created_at', 'updated_at', 'update_ts'
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
