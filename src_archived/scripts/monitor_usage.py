#!/usr/bin/env python
import psutil
import pandas as pd
import time
import os

from core.app_base import AppBase
from libs.telegram_utils import TelegramUtils


class MonitorUsage(AppBase):
    def __init__(self, config):
        super(MonitorUsage, self).__init__(config)
        self.telegram_conf = self.get_param_config(['telegram'])
        self.telegram_utils = TelegramUtils(self.telegram_conf)
        self.latest_alarm = None
        self.pid = str(os.getpid())

    def stats(self):
        # gives a single float value
        cpu_usage = psutil.cpu_percent()
        memory = psutil.virtual_memory()

        # you can have the percentage of used RAM
        memory_percent = memory.percent
        # you can calculate percentage of available memory
        memory_available_percent = memory.available * 100 / psutil.virtual_memory().total

        rs = list()
        for p in psutil.process_iter():
            try:
                rs.append((p.pid, p.name(), p.username(), p.memory_info()[0] / 1024 / 1024 / 1024, p.cpu_percent()))
            except psutil.AccessDenied:
                continue
        df_monitor = pd.DataFrame(rs, columns=['pid', 'name', 'user', 'memory', 'cpu_percent'])
        df_report = df_monitor.rename(
            columns={"memory": "memory_gb"}
        ).groupby(['name'])['memory_gb'].sum().reset_index()
        df_warning = df_report.loc[
            df_report['memory_gb'] > psutil.virtual_memory().total * 0.6 / 1024 / 1024 / 1024]
        df_critical = df_report.loc[
            df_report['memory_gb'] > psutil.virtual_memory().total * 0.75 / 1024 / 1024 / 1024]

        if df_critical.shape[0] > 0:
            if self.latest_alarm is not None:
                cur_time = time.time()
                delta_time = cur_time - self.latest_alarm
                if delta_time < 10:
                    print("ignore")
                    return
            message = """*CRITICAL | cpu {} | mem {} | mem available {}* 
  ```{}```""".format(
                int(cpu_usage), int(memory_percent), int(memory_available_percent), df_critical.to_markdown())
            self.telegram_utils.send_message(self.telegram_conf['owner']['id'], message)
            self.latest_alarm = time.time()

            df_kill = df_monitor.loc[
                df_monitor['name'].isin(df_critical['name'].tolist())
            ]
            for _, r in df_kill.iterrows():
                if str(r['pid']) != self.pid:
                    p = psutil.Process(r['pid'])
                    p.kill()

        elif df_warning.shape[0] > 0:
            if self.latest_alarm:
                cur_time = time.time()
                delta_time = cur_time - self.latest_alarm
                if delta_time < 300:
                    print("ignore")
                    return
            # df_warning.to_markdown()
            message = """*WARNING | cpu {} | mem {} | mem available {}* 
  ```{}```""".format(
                int(cpu_usage), int(memory_percent), int(memory_available_percent), df_warning.to_markdown())
            self.telegram_utils.send_message(self.telegram_conf['owner']['id'], message)
            self.latest_alarm = time.time()

    def execute(self):
        while True:
            try:
                self.stats()
                time.sleep(10)
            except psutil.NoSuchProcess:
                continue
            except psutil.ZombieProcess:
                continue
