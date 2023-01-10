from dwh.pg.pg_base import PgBase


class ErpCrmLead(PgBase):
    def __init__(self, config):
        super(ErpCrmLead, self).__init__(config)
        self.columns = [
            'id', 'name', 'user_id', 'company_id', 'referred', 'description', 'active', 'type', 'priority',
            'team_id', 'stage_id', 'color', 'expected_revenue', 'recurring_revenue', 'recurring_plan',
            'recurring_revenue_monthly', 'recurring_revenue_monthly_prorated', 'date_closed', 'date_action_last',
            'date_open', 'day_open', 'day_close', 'date_last_stage_update', 'date_conversion', 'date_deadline',
            'partner_id', 'contact_name', 'partner_name', 'function', 'title', 'email_from', 'phone', 'mobile',
            'phone_state', 'email_state', 'website', 'lang_id', 'street', 'street2', 'zip', 'city', 'state_id',
            'country_id', 'probability', 'automated_probability', 'lost_reason', 'campaign_id', 'source_id',
            'medium_id', 'email_normalized', 'message_bounce', 'email_cc', 'message_main_attachment_id',
            'phone_sanitized', 'create_uid', 'create_date', 'write_uid', 'write_date', 'won_status',
            'days_to_convert', 'days_exceeding_closing', 'reveal_id', 'deal_ref', 'company_ref', 'customer_ref',
            'partner_type_id', 'contact_email', 'contact_email_2', 'contact_phone', 'contact_phone_2',
            'contact_state_id', 'last_contact_date', 'signup_date', 'last_connected_date', 'stage_state_id',
            'callback_schedule', 'followup_result_l0_id', 'recruitment_demand', 'lost_reason_description',
            'followup_result_l2_id', 'date_feedback_l2', 'followup_result_l4_id', 'followup_result_l5_id',
            'registration_form_ref', 'contract_ref', 'vat', 'payment_method_id', 'payment_schedule_date',
            'followup_result_l7_id', 'is_sync', 'probability_level_id', 'expected_date_moved0', 'reason_of_update',
            'company_name2', 'email_contact', 'email1', 'email2', 'registration_form_ref_is_null_l678',
            'deal_ref_old', 'x_studio_sv_customer_name', 'ai_score', 'is_activities_done', 'activity_date_deadline',
            'is_activities', 'prorated_revenue', 'forecast_revenue', 'tax_rate', 'status_freemium', 'status_pro',
            'is_feedback_sent', 'expected_date', 'is_active_reason_sent', 'active_reason_id', 'active_reason_note',
            'close_date_deadline', 'deadline_to_close_condition', 'show_btn_update_results', 'is_send_result']
        self.fix_dts = [
            'date_closed', 'date_action_last', 'date_open', 'date_last_stage_update', 'date_conversion',
            'date_deadline', 'create_date', 'write_date', 'last_contact_date', 'signup_date',
            'last_connected_date', 'date_feedback_l2', 'payment_schedule_date', 'activity_date_deadline',
            'expected_date', 'close_date_deadline']
        self.cast_float = [
            'active', 'day_open', 'day_close', 'probability', 'automated_probability',
            'days_to_convert', 'days_exceeding_closing', 'is_sync',
            'registration_form_ref_is_null_l678', 'is_activities_done', 'is_activities',
            'forecast_revenue', 'tax_rate', 'is_feedback_sent', 'is_active_reason_sent',
            'deadline_to_close_condition', 'show_btn_update_results', 'is_send_result',
            'ai_score', 'expected_revenue', 'prorated_revenue'
        ]
        self.pg_conf = self.get_param_config(['db', 'erp-prod'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
