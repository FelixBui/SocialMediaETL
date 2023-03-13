from dateutil import tz

from dwh.matomo.matomo_base import MatomoBase


local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class MatomoLogConversionItem(MatomoBase):
    def __init__(self, config):
        super(MatomoLogConversionItem, self).__init__(config)
        self.columns = [
            'idsite', 'idvisitor', 'server_time', 'idvisit', 'idorder', 'idaction_sku', 'idaction_name',
            'idaction_category', 'idaction_category2', 'idaction_category3', 'idaction_category4',
            'idaction_category5', 'price', 'quantity', 'deleted']
        self.fix_dts = ['server_time']
        self.cast_float = ['price', ]
        self.mysql_conf = self.get_param_config(['db', 'matomo-prod'])
        self.column_seed = self.get_param_config(["column_seed"])
