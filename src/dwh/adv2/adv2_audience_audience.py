from dwh.adv2.adv2_base import Adv2Base


class Adv2AudienceAudience(Adv2Base):
    def __init__(self, config):
        super(Adv2AudienceAudience, self).__init__(config)
        self.columns = [
            'id', 'uid', 'channel_code', 'type', 'device_id', 'verify_email', 'verify_phone', 'birthday', 'gender',
            'marital_status', 'sign_up_date', 'login_in_date', 'access_date', 'province_id', 'user_status',
            'employer_company_size', 'employer_fields_activity', 'employer_vip_status', 'seeker_total_resume_step',
            'seeker_total_resume_file', 'seeker_total_resume_applied', 'seeker_resume_applied_at',
            'resume_current_position', 'resume_position', 'resume_experience', 'resume_province_ids',
            'resume_field_ids', 'resume_salary', 'resume_level', 'created_at', 'updated_at']
        self.fix_dts = ['birthday', 'sign_up_date', 'login_in_date', 'access_date', 'seeker_resume_applied_at']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
