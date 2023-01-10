from dwh.pg.pg_base import PgBase


class ErpAccountMoveLine(PgBase):
    def __init__(self, config):
        super(ErpAccountMoveLine, self).__init__(config)
        self.columns = [
            'id', 'move_id', 'move_name', 'date', 'ref', 'parent_state', 'journal_id', 'company_id',
            'company_currency_id', 'account_id', 'account_root_id', 'sequence', 'name', 'quantity',
            'price_unit', 'discount', 'debit', 'credit', 'balance', 'amount_currency', 'price_subtotal',
            'price_total', 'reconciled', 'blocked', 'date_maturity', 'currency_id', 'partner_id',
            'product_uom_id', 'product_id', 'reconcile_model_id', 'payment_id', 'statement_line_id',
            'statement_id', 'tax_line_id', 'tax_group_id', 'tax_base_amount', 'tax_exigible',
            'tax_repartition_line_id', 'tax_audit', 'amount_residual', 'amount_residual_currency',
            'full_reconcile_id', 'matching_number', 'analytic_account_id', 'display_type',
            'is_rounding_line', 'exclude_from_invoice_tab', 'create_uid', 'create_date',
            'write_uid', 'write_date', 'expected_pay_date', 'internal_note', 'next_action_date',
            'followup_line_id', 'followup_date', '"lineID"', 'is_transfer', 'is_netoff', 'einvoice_number',
            'einvoice_date', 'countered_accounts', 'cost_element_id', 'expense_id']
        self.fix_dts = [
            'date', 'date_maturity', 'create_date', 'write_date', 'expected_pay_date',
            'next_action_date', 'followup_date', 'einvoice_date']
        self.cast_float = [
            'reconciled', 'blocked', 'tax_exigible', 'is_rounding_line',
            'exclude_from_invoice_tab', 'is_transfer', 'is_netoff', 'price_unit',
            'quantity'
        ]
        self.pg_conf = self.get_param_config(['db', 'erp-prod'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
