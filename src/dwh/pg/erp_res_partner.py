from dwh.pg.pg_base import PgBase


class ErpResPartner(PgBase):
    def __init__(self, config):
        super(ErpResPartner, self).__init__(config)
        self.columns = [
            'id', 'name', 'company_id', 'create_date', 'display_name', 'date', 'title', 'parent_id', 'ref',
            'lang', 'tz', 'user_id', 'vat', 'website', 'comment', 'credit_limit', 'active', 'employee',
            'function', 'type', 'street', 'street2', 'zip', 'city', 'state_id', 'country_id', 'partner_latitude',
            'partner_longitude', 'email', 'phone', 'mobile', 'is_company', 'industry_id', 'color', 'partner_share',
            'commercial_partner_id', 'commercial_company_name', 'company_name', 'create_uid', 'write_uid',
            'write_date', 'message_main_attachment_id', 'email_normalized', 'message_bounce',
            'contact_address_complete', 'signup_token', 'signup_type', 'signup_expiration',
            'calendar_last_notif_ack', 'team_id', 'ocn_token', 'partner_gid', 'additional_info',
            'phone_sanitized', 'debit_limit', 'last_time_entries_checked', 'invoice_warn', 'invoice_warn_msg',
            'supplier_rank', 'customer_rank', 'online_partner_vendor_name', 'online_partner_bank_account',
            'company_type', 'partner_type_id', 'partner_scale_id', 'is_sync', 'last_contact_date', 'signup_date',
            'last_connected_date', 'medium_id', 'assigned_date', 'is_internal_partner', 'mobile2', 'mobile3',
            'sale_warn', 'sale_warn_msg', '"customerName"', 'company_name2', 'email_contact', 'email1', 'email2',
            'no_opportunity', 'ai_score', 'campaign_id', 'tracking_emails_count', 'email_score', 'email_bounced',
            'lead_in_l0_to_l7', 'employer_classification', 'employee_size', 'company_size', 'areas_of_work',
            'establish_date', 'is_headhunt', 'source_id', '"contactID"', 'status_freemium', 'status_pro',
            'freemium_score', 'other_info', 'current_channel', 'used_channel', 'channel_on_sv', 'email_mkt_status',
            'applied_cv', 'tracking_activity_type_id', 'tracking_summary', 'tracking_date_deadline', 'tracking_note',
            'tracking_user_id', 'tracking_create_uid', 'tracking_create_date', 'email_status']
        self.fix_dts = [
            'create_date', 'date', 'write_date', 'signup_expiration', 'calendar_last_notif_ack',
            'last_time_entries_checked', 'last_contact_date', 'signup_date', 'last_connected_date',
            'assigned_date', 'establish_date', 'tracking_date_deadline', 'tracking_create_date']
        self.cast_float = [
            'credit_limit', 'active', 'employee', 'is_company', 'partner_share', 'is_sync',
            'is_internal_partner', 'no_opportunity', 'email_score', 'email_bounced', 'is_headhunt',
            'freemium_score', 'ai_score', 'partner_latitude', 'partner_longitude']

        self.pg_conf = self.get_param_config(['db', 'erp-prod'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
