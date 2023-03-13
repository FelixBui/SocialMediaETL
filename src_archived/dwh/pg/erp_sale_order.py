from dwh.pg.pg_base import PgBase


class ErpSaleOrder(PgBase):
    def __init__(self, config):
        super(ErpSaleOrder, self).__init__(config)
        self.columns = [
            'id', 'campaign_id', 'source_id', 'medium_id', 'message_main_attachment_id',
            'name', 'origin', 'client_order_ref', 'reference', 'state', 'date_order', 'validity_date',
            'require_signature', 'require_payment', 'create_date', 'user_id', 'partner_id',
            'partner_invoice_id', 'partner_shipping_id', 'pricelist_id', 'currency_id', 'analytic_account_id',
            'invoice_status', 'note', 'amount_untaxed', 'amount_tax', 'amount_total', 'currency_rate',
            'payment_term_id', 'fiscal_position_id', 'company_id', 'team_id', 'signed_by', 'signed_on',
            'commitment_date', 'show_update_pricelist', 'create_uid', 'write_uid', 'write_date',
            'sale_order_template_id', 'opportunity_id', '"SalesOrderUuid"', '"paymentType"', '"invoiceStatus"',
            'company_branch_id', 'date_call_api', '"svcustomerName"']
        self.fix_dts = [
            'date_order', 'validity_date', 'create_date', 'signed_on',
            'commitment_date', 'write_date', 'date_call_api']
        self.cast_float = ['require_signature', 'require_payment', 'show_update_pricelist']
        self.pg_conf = self.get_param_config(['db', 'erp-prod'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
