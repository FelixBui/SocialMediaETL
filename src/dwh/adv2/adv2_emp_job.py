from dwh.adv2.adv2_base import Adv2Base


class Adv2EmpJob(Adv2Base):
    def __init__(self, config):
        super(Adv2EmpJob, self).__init__(config)
        self.columns = [
            'id', 'employer_id', 'status', 'last_revision_status', 'channel_code', 'ref_id',
            'title', 'title_slug', 'level_requirement', 'field_ids_main', 'field_ids_sub', 'field_ids_child',
            'province_ids', 'district_ids', 'area', 'gate_code', 'vacancy_quantity', 'description',
            'skill_requirement', 'working_method', 'attribute', 'probation_duration', 'probation_duration_text',
            'salary_range', 'salary_min', 'salary_max', 'salary_unit', 'resume_apply_expired', 'benefit',
            'age_range', 'degree_requirement', 'gender', 'experience_range', 'resume_requirement',
            'language_requirement', 'job_requirement', 'other_requirement', 'commission_from', 'commission_to',
            'contact_name', 'contact_email', 'contact_phone', 'contact_mobile', 'contact_address', 'contact_method',
            'total_views', 'premium_type', 'is_verified', 'is_search_allowed', 'approved_at', 'approved_by',
            'deleted_reason', 'deleted_by', 'deleted_at', 'meta_title', 'meta_description', 'meta_keywords',
            'refresh_at', 'priority_max', 'priority_all', 'total_resume_applied', 'is_forbid', 'created_source',
            'job_post_type', 'created_at', 'created_by', 'updated_at', 'updated_by', 'update_ts']

        self.fix_dts = [
            'resume_apply_expired', 'approved_at', 'deleted_at', 'refresh_at']

        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
