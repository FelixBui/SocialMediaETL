import datetime
import abc
from abc import ABCMeta
from typing import Type
import functools
import logging
import traceback
import signal
import tqdm

from libs.utils import deep_get
from libs.telegram_utils import TelegramUtils

message_success_template = """```
Process: {0}
Status: Success
Duration: {1}
```
"""

message_fail_template = """```
Process: {0}
Status: Fail
Error: {1}
```
"""

message_kill_template = """```
Process: {0}
Status: Kill
At: {1}
```
"""

message_template = """```
Process: {0}
Message: {1}
```
"""


class AppBase(object):
    __metaclass__: Type[ABCMeta] = abc.ABCMeta

    def __init__(self, config):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.config = config
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(format=log_format, level=logging.INFO)
        self.log = logging.getLogger(config['process_name'])

        self.process_type = self.get_process_info(['process_type'])
        self.process_group = self.get_process_info(['process_group'])
        self.process_name = self.get_process_info(['process_name'])

        telegram_config = self.get_param_config(['telegram'])
        self.owner_id = telegram_config['owner']['id']
        self.telegram_bot = TelegramUtils(telegram_config)
        self.execution_date = self.get_process_info(['execution_date'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

    def exit_gracefully(self, *args):
        process_info = "process_type={0} process_group={1} process_name={2}".format(
            self.process_type, self.process_group, self.process_name, self.execution_date)
        at = datetime.datetime.now()
        message = message_kill_template.format(process_info, at.strftime("%Y-%m-%d %H:%M:%S"))
        self.telegram_bot.send_message(
            user_id=self.owner_id,
            msg=message
        )
        exit(0)

    @abc.abstractmethod
    def execute(self):
        """Retrieve data from the input source and return an object."""
        return

    def send_message(self, msg):
        process_info = "ptype={0} pgroup={1} pname={2}".format(
            self.process_type, self.process_group, self.process_name, self.execution_date)
        self.telegram_bot.send_message(
            user_id=self.owner_id,
            msg=message_template.format(process_info, msg)
        )

    def backfill(self, interval_hours=6):
        from_date = self.from_date
        to_date = self.to_date
        seed_date = from_date + datetime.timedelta(hours=interval_hours)
        total = (to_date - from_date) / datetime.timedelta(hours=interval_hours)

        self.log.info("loop total: {}".format(total))
        with tqdm.tqdm(total=total) as pbar:
            while seed_date <= to_date:
                self.from_date = from_date
                self.to_date = seed_date
                self.execute()
                from_date = seed_date
                seed_date += datetime.timedelta(hours=interval_hours)
                pbar.update(1)

    def get_param_config(self, keys: list, option=False):
        default_value = deep_get(self.config, *keys)
        _value = deep_get(self.config['params'], *keys)
        if _value is None:
            if default_value:
                self.log.debug("load " + str(keys) + ": with default " + str(default_value))
                return default_value
            else:
                if option:
                    self.log.debug("not found config with key " + str(keys))
                    return None
                self.log.error("not found config with key " + str(keys))
                raise KeyError("not found config with key " + str(keys))
        else:
            self.log.debug("load " + str(keys) + ": " + str(_value))
            return _value

    def get_process_info(self, keys):
        _value = deep_get(self.config, *keys)
        if _value is None:
            self.log.error("not found config with key " + str(keys))
            raise KeyError("not found config with key " + str(keys))
        else:
            self.log.debug("load " + str(keys) + ": " + str(_value))
            return _value

    def wrapper_simple_log(func):
        @functools.wraps(func)
        def wrap(self, *args, **kwargs):
            process_info = "ptype={0} pgroup={1} pname={2}".format(
                self.process_type, self.process_group, self.process_name, self.execution_date)
            started_at = datetime.datetime.now()
            message = "{0} start at: {1}".format(process_info, started_at.strftime("%Y-%m-%d %H:%M:%S"))
            self.log.info(message)
            try:
                func(self, *args, **kwargs)
            except Exception as e:
                message = process_info + "\n" + traceback.format_exc()
                self.log.error(message)
                self.telegram_bot.send_message(
                    user_id=self.owner_id,
                    msg=message_fail_template.format(process_info, message[:2000])
                )
                raise e
            finished_at = datetime.datetime.now()
            message = "{0} stop at: {1}".format(process_info, finished_at.strftime("%Y-%m-%d %H:%M:%S"))
            self.log.info(message)
            duration = finished_at - started_at
            message = "{0} duration time: {1}".format(process_info, duration)
            self.log.info(message)

            self.telegram_bot.send_message(
                user_id=self.owner_id,
                msg=message_success_template.format(process_info, duration)
            )
        return wrap

    wrapper_simple_log = staticmethod(wrapper_simple_log)
