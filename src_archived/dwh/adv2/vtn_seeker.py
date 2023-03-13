from dwh.adv2.adv2_base import Adv2Base


class VtnSeeker(Adv2Base):
    def __init__(self, config):
        super(VtnSeeker, self).__init__(config)
        self.columns = [
            'id', 'email', 'phone_login', 'password', 'name', 'slug', 'gender', 'birthday', 'address',
            'province', 'phone', 'mobile', 'identification', 'marital_status', 'avatar', 'status', 'token',
            'logined_at', 'logined_ip', 'created_at', 'updated_at', 'actived_at', 'is_premium',
            'premium_created_at', 'premium_renewed_at', 'premium_expired_at', 'admin_username',
            'admin_callcenter_username', 'admin_viewed_at', 'is_mail_birthday_received',
            'is_mail_notify_received', 'cover_status', 'cover_token', 'cover_avatar', 'cover_image',
            'cover_expired_at', 'from_id', 'from_source', 'salt', 'token_email', 'token_sms', 'is_supported',
            'seeker_support_type', 'user_approved', 'edit_status', 'reason_status', 'old_data', 'created_source',
            'seeker_support_note', 'user_approve_support', 'resume_count', 'google_id', 'facebook_id',
            'area_register', 'is_covid19', 'type_covid19']
        self.fix_dts = ['birthday', 'logined_at', 'created_at', 'updated_at', 'actived_at',
                        'admin_viewed_at', 'cover_expired_at']

        self.mysql_conf = self.get_param_config(['db', 'vtn'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
