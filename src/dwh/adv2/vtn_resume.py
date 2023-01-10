from dwh.adv2.adv2_base import Adv2Base


class VtnResume(Adv2Base):
    def __init__(self, config):
        super(VtnResume, self).__init__(config)
        self.columns = [
            'id', 'seeker_id', 'career_obj', 'title', 'slug', 'position', 'position_current', 'wanting_job',
            'skill', 'interesting', 'special_skill', 'level', 'experience', 'computer_level', 'language',
            'language_level', 'current_salary', 'salary', 'salary_unit', 'salary_min', 'work_time', 'fields',
            'provinces', 'total_views', 'is_ready_move', 'is_search_allowed', 'is_finished', 'completion_date',
            'status', 'created_at', 'updated_at', 'auto_inactived_at', 'actived_at', 'actived_admin_username',
            'feedback_at', 'feedback_admin_username', 'feedback_content', 'feedback_reason', 'edit_status',
            'c_resume_id', 'requirement_job', 'resume_kind', 'resume_file', 'status_yes', 'source',
            'data_search', 'from_id', 'from_source', 'create_source', 'completion_source', 'user_delete',
            'view_order', 'created_by_username', 'is_covid19', 'type_covid19']
        self.fix_dts = [
            'completion_date', 'created_at', 'updated_at',
            'auto_inactived_at', 'actived_at', 'feedback_at']

        self.mysql_conf = self.get_param_config(['db', 'vtn'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
