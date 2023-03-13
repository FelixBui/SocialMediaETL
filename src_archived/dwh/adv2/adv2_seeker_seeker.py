from dwh.adv2.adv2_base import Adv2Base


class Adv2SeekerSeeker(Adv2Base):
    def __init__(self, config):
        super(Adv2SeekerSeeker, self).__init__(config)
        self.columns = [
            'id', 'channel_code', 'branch_code', 'status', 'last_revision_status', 'resume_status',
            'google_id', 'facebook_id', 'email', 'password', 'verify_password', 'hash_password_type',
            'token_password_reset', 'token_password_reset_expired', 'name', 'slug', 'gender',
            'marital_status', 'birthday', 'address', 'mobile', 'province_id', 'avatar', 'logined_at',
            'logined_ip', 'assigned_staff_id', 'assigned_staff_login_name', 'assigned_staff_at',
            'token', 'token_email', 'token_sms', 'resume_count', 'support_type', 'support_note',
            'support_approved_by', 'created_at', 'created_by', 'updated_at', 'updated_by',
            'created_source', 'branch_register', 'update_ts']
        self.fix_dts = ['assigned_staff_at', 'logined_at', 'birthday']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
