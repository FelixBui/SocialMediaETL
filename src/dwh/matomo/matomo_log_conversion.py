from dateutil import tz

from dwh.matomo.matomo_base import MatomoBase


local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class MatomoLogConversion(MatomoBase):
    def __init__(self, config):
        super(MatomoLogConversion, self).__init__(config)
        self.columns = [
            'idvisit', 'idsite', 'idvisitor', 'server_time', 'idaction_url', 'idlink_va',
            'idgoal', 'buster', 'idorder', 'items', 'url', 'revenue', 'revenue_shipping',
            'revenue_subtotal', 'revenue_tax', 'revenue_discount', 'visitor_returning',
            'visitor_seconds_since_first', 'visitor_seconds_since_order', 'visitor_count_visits',
            'referer_keyword', 'referer_name', 'referer_type', 'config_browser_name', 'config_client_type',
            'config_device_brand', 'config_device_model', 'config_device_type', 'location_city',
            'location_country', 'location_latitude', 'location_longitude', 'location_region',
            'custom_dimension_1', 'custom_dimension_2', 'custom_dimension_3', 'custom_dimension_4',
            'custom_dimension_5', 'campaign_content', 'campaign_group', 'campaign_id', 'campaign_keyword',
            'campaign_medium', 'campaign_name', 'campaign_placement', 'campaign_source', 'custom_var_k1',
            'custom_var_v1', 'custom_var_k2', 'custom_var_v2', 'custom_var_k3', 'custom_var_v3', 'custom_var_k4',
            'custom_var_v4', 'custom_var_k5', 'custom_var_v5']
        self.fix_dts = ['server_time']
        self.cast_float = [
            'revenue', 'revenue_shipping', 'revenue_subtotal', 'revenue_tax',
            'revenue_discount', 'location_latitude', 'location_longitude']
        self.mysql_conf = self.get_param_config(['db', 'matomo-prod'])
        self.column_seed = self.get_param_config(["column_seed"])
