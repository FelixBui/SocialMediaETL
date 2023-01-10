from dateutil import tz

from dwh.matomo.matomo_base import MatomoBase


local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class MatomoLogAction(MatomoBase):
    def __init__(self, config):
        super(MatomoLogAction, self).__init__(config)
        self.columns = ['idaction', 'name', 'hash', 'type', 'url_prefix']
        self.mysql_conf = self.get_param_config(['db', 'matomo-prod'])
        self.column_seed = self.get_param_config(["column_seed"])
