from dateutil import tz

from core.app_base import AppBase

local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class TaskDemo(AppBase):
    def __init__(self, config):
        super().__init__(config)
        self.message = self.get_param_config(['message'])

    @AppBase.wrapper_simple_log
    def execute(self):
        print("message: ", self.message)
        print(self.execution_date.astimezone(utc_tz))
        print(self.execution_date.astimezone(local_tz))
