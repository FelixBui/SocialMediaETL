from dwh.adv2.adv2_base import Adv2Base


class VtnJob(Adv2Base):
    def __init__(self, config):
        super(VtnJob, self).__init__(config)
        self.columns = [
            'id', 'employer_id', 'title', 'slug', 'quantity', 'gender', 'description',
            'skill', 'attribute', 'level', 'experience', 'salary', 'salary_unit', 'work_time',
            'probation_time', 'benefit', 'resume_requirements', 'fields', 'provinces', 'districts',
            'total_views', 'is_premium', 'is_search_allowed', 'status', 'position', 'tags',
            'created_at', 'updated_at', 'expired_at', 'contact_name', 'contact_email',
            'contact_address', 'contact_address_new', 'contact_phone', 'contact_mobile',
            'contact_type', 'actived_at', 'actived_admin_username', 'feedback_at',
            'feedback_admin_username', 'feedback_content', 'feedback_reason', 'is_paid', 'edit_status',
            'area', 'image', 'source', 'min_kickback', 'max_kickback', 'is_premium2', 'from_source',
            'from_id', 'converted_at', 'is_forbid', 'refresh_total', 'data_search', 'age',
            'employer_contact_id', 'created_admin_id']

        self.fix_dts = ['created_at', 'updated_at', 'expired_at', 'actived_at', 'feedback_at', 'converted_at']

        self.mysql_conf = self.get_param_config(['db', 'vtn'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
