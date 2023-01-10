from dwh.adv2.adv2_base import Adv2Base


class Adv2MixResumeAppliedHistory(Adv2Base):
    def __init__(self, config):
        super(Adv2MixResumeAppliedHistory, self).__init__(config)
        self.columns = [
            'id', 'channel_code', 'job_id', 'employer_id', 'resume_id', 'seeker_id', 'file_name',
            'file_name_hidden', 'file_name_doc', 'recruitment_status', 'status', 'resume_type',
            'rejected_kind', 'rejected_reason', 'rejected_reason_other', 'created_at', 'updated_at',
            'created_source', 'seeker_deleted', 'employer_deleted', 'applied_at', 'approved_at', 'is_new',
            'applied_status', 'apply_job_it', 'resume_file_status', 'update_ts']
        self.fix_dts = ['applied_at', 'approved_at']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
