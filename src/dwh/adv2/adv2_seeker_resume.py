from dwh.adv2.adv2_base import Adv2Base


class Adv2SeekerResume(Adv2Base):
    def __init__(self, config):
        super(Adv2SeekerResume, self).__init__(config)
        self.columns = [
            'id', 'channel_code', 'branch_code', 'seeker_id', 'status', 'last_revision_status', 'title', 'slug',
            'resume_type', 'level', 'position', 'current_position', 'career_objective', 'field_ids', 'province_ids',
            'work_time', 'experience', 'language', 'current_salary', 'salary_range', 'min_expected_salary',
            'max_expected_salary', 'skills', 'cover_letter', 'cv_file', 'total_views', 'is_search_allowed',
            'refreshed_at', 'created_from', 'completed_at', 'source_sync', 'id_ref', 'created_by', 'created_at',
            'updated_by', 'updated_at', 'created_source', 'approved_at', 'update_ts']
        self.fix_dts = ['refreshed_at', 'completed_at', 'created_at', 'approved_at', 'updated_at']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
