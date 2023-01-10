from dwh.pg.pg_base import PgBase


class ErpRevenueRecognize(PgBase):
    def __init__(self, config):
        super(ErpRevenueRecognize, self).__init__(config)
        self.columns = [
            'id', 'name', '"idItem"', '"idSO_moved0"', '"idSubItem"', 'revenue',
            'revenue_date', '"currencyCode"', 'sale_order_id', 'sale_order_line_id',
            'product_id', 'company_id', 'state', 'create_uid', 'create_date', 'write_uid',
            'write_date', '"idSO"', 'account_move_id', 'seller_id', 'message_main_attachment_id']
        self.fix_dts = ['revenue_date', 'create_date', 'write_date']
        self.cast_float = ['revenue', ]
        self.pg_conf = self.get_param_config(['db', 'erp-prod'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
