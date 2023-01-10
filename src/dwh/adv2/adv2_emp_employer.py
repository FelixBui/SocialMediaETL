from dwh.adv2.adv2_base import Adv2Base


class Adv2EmpEmployer(Adv2Base):
    def __init__(self, config):
        super(Adv2EmpEmployer, self).__init__(config)
        self.columns = ['id', 'channel_code', 'channel_checkmate', 'branch_code', 'email',
                        'password', 'verify_password', 'password_encryption', 'token', 'name',
                        'slug', 'description', 'address', 'province_id', 'phone', 'id_card',
                        'folder', 'folder_at', 'minisite_slug', 'logo', 'website', 'fax',
                        'tax_code', 'number_of_employer', 'company_size', 'company_kind',
                        'staff_age_range', 'marked_as_top', 'founded_year', 'latitude',
                        'longitude', 'contact_name', 'contact_email', 'contact_address',
                        'contact_phone', 'contact_method', 'business_license_file',
                        'business_license_status', 'business_license_rejected_reason',
                        'business_license_upload_at', 'business_license_approved_by',
                        'rival_type', 'fields_activity', 'status', 'last_revision_status',
                        'suspect_status', 'recontract_status', 'email_verified_status',
                        'email_verified_at', 'email_verified_source', 'email_verified_token',
                        'deleted_note', 'deleted_by', 'deleted_at', 'last_logged_in_at',
                        'last_logged_in_ip', 'premium_job_status', 'premium_status',
                        'premium_created_at', 'premium_renewed_at', 'premium_end_at',
                        'total_buy_point', 'total_remaining_buy_point',
                        'total_remaining_gift_point', 'buy_point_end_on', 'gift_point_end_on',
                        'buy_point_reserved_status', 'buy_point_reserved_on', 'assigned_type',
                        'assigned_staff_id', 'assigned_staff_username', 'assigning_changed_at',
                        'customer_id', 'customer_status', 'account_type',
                        'email_marketing_status', 'throwout_type', 'room_id',
                        'employer_classification', 'sales_order_approved_at', 'data_source',
                        'created_source', 'support_info', 'created_at', 'created_by',
                        'updated_at', 'updated_by', 'update_ts', 'cross_sale_assign_id',
                        'account_service_assigned_id', 'account_service_assigned_username']
        self.fix_dts = [
            'business_license_upload_at', 'last_logged_in_at', 'folder_at', 'deleted_at',
            'gift_point_end_on', 'buy_point_reserved_on', 'last_logged_in_at',
            'email_verified_at', 'deleted_at', 'buy_point_end_on', 'premium_created_at',
            'premium_end_at'
        ]
        self.cast_float = [
            'latitude', 'longitude'
        ]
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
