from dwh.adv2.adv2_base import Adv2Base


class Adv2AuthStaff(Adv2Base):
    def __init__(self, config):
        super(Adv2AuthStaff, self).__init__(config)
        self.columns = [
            'id', 'channel_code', 'code', 'login_name', 'password', 'verify_password',
            'hash_password_type', 'email', 'display_name', 'avatar_path', 'address',
            'phone', 'xlite_id', 'division_code', 'data_group_code', 'language_code',
            'status', 'customer_care_level', 'mode', 'employer_care_type', 'google_secret',
            'start_working_date', 'end_working_date', 'created_by', 'created_at',
            'updated_by', 'updated_at', 'created_source', 'token', 'token_reset',
            'token_reset_expired', 'note', 'update_ts']
        self.fix_dts = ['created_at', 'start_working_date', 'end_working_date', 'token_reset_expired']
        self.mysql_conf = self.get_param_config(['db', 'adv2_epsilon'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
