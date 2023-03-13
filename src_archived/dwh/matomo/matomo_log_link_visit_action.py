from dateutil import tz

from dwh.matomo.matomo_base import MatomoBase
from libs.storage_utils import load_df_from_bq, load_df_from_mysql


local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class MatomoLogLinkVisitAction(MatomoBase):

    def __init__(self, config):
        super(MatomoLogLinkVisitAction, self).__init__(config)
        self.columns = [
            'idlink_va', 'idsite', 'idvisitor', 'idvisit', 'idaction_url_ref', 'idaction_name_ref',
            'custom_float', 'pageview_position', 'server_time', 'idpageview', 'idaction_name',
            'idaction_url', 'search_cat', 'search_count', 'time_spent_ref_action',
            'idaction_product_cat', 'idaction_product_cat2', 'idaction_product_cat3',
            'idaction_product_cat4', 'idaction_product_cat5', 'idaction_product_name',
            'product_price', 'idaction_product_sku', 'idaction_event_action',
            'idaction_event_category', 'idaction_content_interaction', 'idaction_content_name',
            'idaction_content_piece', 'idaction_content_target', 'time_dom_completion',
            'time_dom_processing', 'time_network', 'time_on_load', 'time_server',
            'time_transfer', 'time_spent', 'custom_dimension_1', 'custom_dimension_2',
            'custom_dimension_3', 'custom_dimension_4', 'custom_dimension_5', 'custom_var_k1',
            'custom_var_v1', 'custom_var_k2', 'custom_var_v2', 'custom_var_k3', 'custom_var_v3',
            'custom_var_k4', 'custom_var_v4', 'custom_var_k5', 'custom_var_v5']
        self.cast_dt = ['server_time', ]
        self.cast_float = ['custom_float', 'product_price']
        self.mysql_conf = self.get_param_config(['db', 'matomo-prod'])
        self.column_seed = self.get_param_config(["column_seed"])
