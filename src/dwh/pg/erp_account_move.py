from dwh.pg.pg_base import PgBase


class ErpAccountMove(PgBase):
    def __init__(self, config):
        super(ErpAccountMove, self).__init__(config)
        self.columns = [
            'id', 'name', 'date', 'ref', 'narration', 'state', 'posted_before', 'move_type',
            'to_check', 'journal_id', 'company_id', 'currency_id', 'partner_id',
            'commercial_partner_id', 'is_move_sent', 'partner_bank_id', 'payment_reference',
            'payment_id', 'statement_line_id', 'amount_untaxed', 'amount_tax', 'amount_total',
            'amount_residual', 'amount_untaxed_signed', 'amount_tax_signed', 'amount_total_signed',
            'amount_residual_signed', 'payment_state', 'tax_cash_basis_rec_id', 'tax_cash_basis_move_id',
            'auto_post', 'reversed_entry_id', 'fiscal_position_id', 'invoice_user_id', 'invoice_date',
            'invoice_date_due', 'invoice_origin', 'invoice_payment_term_id', 'invoice_incoterm_id',
            'qr_code_method', 'invoice_source_email', 'invoice_partner_display_name',
            'invoice_cash_rounding_id', 'secure_sequence_number', 'inalterable_hash',
            'message_main_attachment_id', 'sequence_prefix', 'sequence_number', 'create_uid',
            'create_date', 'write_uid', 'write_date', 'edi_state', 'duplicated_vendor_ref',
            'extract_state', 'extract_status_code', 'extract_remote_id', 'is_sync',
            'payment_state_before_switch', 'transfer_model_id', 'tax_closing_end_date',
            'tax_report_control_error', 'asset_id', 'asset_remaining_value', 'asset_depreciated_value',
            'asset_manually_modified', 'asset_value_change', 'campaign_id', 'source_id', 'medium_id',
            'partner_shipping_id', 'vsi_status', 'so_id', '"buyerName"', 'revenue_recognize_id',
            'vsi_template', 'vsi_series', 'vsi_number', 'tas_type', 'ma_phieu_in', 'origin',
            'kemtheo', 'lydo', 'address', 'nguoi_lap', 'nguoi_nhan', 'manager_id', 'accountant_id',
            'treasurer_id', '"svcustomerName"', 'team_id', 'company_branch_vat', 'company_branch_id',
            'move_transfer_id', 'move_netoff_id', 'einvoice_date', 'is_separated', 'tax_declaration',
            'invoice_address', 'buyer_vat', 'invoice_type', 'currency_rate', 'expense_id']
        self.fix_dts = [
            'date', 'invoice_date', 'invoice_date_due', 'create_date', 'write_date',
            'tax_closing_end_date', 'einvoice_date']
        self.cast_float = [
            'posted_before', 'to_check', 'is_move_sent', 'auto_post', 'is_sync', 'tax_report_control_error',
            'asset_manually_modified', 'asset_value_change', 'is_separated', 'tax_declaration', 'currency_rate']
        self.pg_conf = self.get_param_config(['db', 'erp-prod'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
