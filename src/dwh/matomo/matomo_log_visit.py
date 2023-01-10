from dateutil import tz
import datetime

from dwh.matomo.matomo_base import MatomoBase
from libs.storage_utils import load_df_from_mysql, load_df_from_bq


local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class MatomoLogVisit(MatomoBase):

    def __init__(self, config):
        super(MatomoLogVisit, self).__init__(config)
        self.columns = [
            'idvisit', 'idsite', 'idvisitor', 'visit_last_action_time', 'config_id',
            'location_ip', 'profilable', 'user_id', 'visit_first_action_time',
            'visit_goal_buyer', 'visit_goal_converted', 'visitor_returning',
            'visitor_seconds_since_first', 'visitor_seconds_since_order', 'visitor_count_visits',
            'visit_entry_idaction_name', 'visit_entry_idaction_url', 'visit_exit_idaction_name',
            'visit_exit_idaction_url', 'visit_total_actions', 'visit_total_interactions',
            'visit_total_searches', 'referer_keyword', 'referer_name', 'referer_type',
            'referer_url', 'location_browser_lang', 'config_browser_engine', 'config_browser_name',
            'config_browser_version', 'config_client_type', 'config_device_brand', 'config_device_model', 'config_device_type', 'config_os', 'config_os_version', 'visit_total_events', 'visitor_localtime', 'visitor_seconds_since_last', 'config_resolution', 'config_cookie', 'config_flash', 'config_java', 'config_pdf', 'config_quicktime', 'config_realplayer', 'config_silverlight', 'config_windowsmedia', 'visit_total_time', 'location_city', 'location_country', 'location_latitude', 'location_longitude', 'location_region', 'last_idlink_va', 'custom_dimension_1', 'custom_dimension_2', 'custom_dimension_3', 'custom_dimension_4', 'custom_dimension_5', 'campaign_content', 'campaign_group', 'campaign_id', 'campaign_keyword', 'campaign_medium', 'campaign_name', 'campaign_placement', 'campaign_source', 'custom_var_k1', 'custom_var_v1', 'custom_var_k2', 'custom_var_v2', 'custom_var_k3', 'custom_var_v3', 'custom_var_k4', 'custom_var_v4', 'custom_var_k5', 'custom_var_v5', 'location_provider']
        self.cast_dt = ['visit_last_action_time', 'visit_first_action_time']
        self.fix_dts = ['visitor_localtime', ]
        self.cast_float = ['location_latitude', 'location_longitude']
        self.mysql_conf = self.get_param_config(['db', 'matomo-prod'])
        self.column_seed = self.get_param_config(["column_seed"])
