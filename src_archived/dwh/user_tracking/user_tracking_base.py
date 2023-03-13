from abc import ABC

from dateutil import tz
from core.app_base import AppBase
import pytz

local_tz = tz.tzlocal()
sg_tz = pytz.timezone("Asia/Saigon")
utc_tz = tz.tzutc()


class UserTrackingBase(AppBase, ABC):

    @AppBase.wrapper_simple_log
    def backfill(self, interval_hours=1):
        self.log.info(
        """
        mode: backfill
        from_date: {}
        to_date: {}"
        interval_hours: {}
        """.format(self.from_date, self.to_date, interval_hours))
        super(UserTrackingBase, self).backfill(interval_hours)
