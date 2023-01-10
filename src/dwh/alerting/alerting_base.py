from core.app_base import AppBase
import slack
import functools

message_fail_template = """```
Process: {0}
Status: Fail
```"""
mode_test = True
slack_channel = "#test_bot" if mode_test else "#data_alert_bot"


class AlertingBase(AppBase):
    def __init__(self, config):
        super().__init__(config)
        slack_token = self.get_param_config(['slack_token'])
        self.slack_client = slack.WebClient(token=slack_token)

    def wrapper_simple_log(func):
        @functools.wraps(func)
        def wrap(self, *args, **kwargs):
            process_info = "pgroup={0} pname={1}".format(
                self.process_group, self.process_name)
            msg = message_fail_template.format(process_info)
            try:
                func(self, *args, **kwargs)
            except Exception as e:
                print(msg)
                self.slack_client.chat_postMessage(channel=slack_channel, text=msg)
                raise e
        return wrap

    wrapper_simple_log = staticmethod(wrapper_simple_log)
