from dwh.adv2.adv2_base import Adv2Base


class Adv2SeekerResumeRevision(Adv2Base):
    def __init__(self, config):
        super(Adv2SeekerResumeRevision, self).__init__(config)
        self.columns = [
            'id', 'channel_code', 'resume_id', 'revision_status', 'resume_status', 'title', 'slug', 'resume_type',
            'level', 'position', 'current_position', 'career_objective', 'field_ids', 'province_ids', 'work_time',
            'experience', 'salary_range', 'min_expected_salary', 'cover_letter', 'cv_file', 'approved_at',
            'approved_by', 'rejected_reason', 'rejected_reason_note', 'resume_it', 'resume_language',
            'resume_consultor', 'resume_diploma', 'resume_experience', 'resume_skill', 'created_at', 'created_by',
            'updated_at', 'updated_by', 'created_source', 'snap_shot', 'update_ts']
        self.fix_dts = ['approved_at']
        self.cast_dt = ['created_at', 'updated_at', 'update_ts']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
