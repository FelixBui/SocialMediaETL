import pandas as pd
import re
from dateutil import tz

from libs.storage_utils import load_df_from_postgres
from jobs.backup.task_backup_s3 import TaskBackupS3

local_tz = tz.tzlocal()
remove_non_alphabet = re.compile('[^a-zA-Z ]')


class TaskBackupCb(TaskBackupS3):

    def extract(self):
        sql_cb_job = "select 'cb' as channel_code, 'job' as sourcetype, " \
                     "id, url, created_at from cb_job " \
                     "where created_at >= '{}' and created_at <= '{}'".format(self.from_date, self.to_date)
        df_cb_job = load_df_from_postgres(
            self.postgres_conf,
            sql_cb_job
        )
        sql_cb_company = "select 'cb' as channel_code, 'company' as sourcetype, " \
                         "id, url, created_at from cb_company " \
                         "where created_at >= '{}' and created_at <= '{}'".format(self.from_date, self.to_date)
        df_cb_company = load_df_from_postgres(
            self.postgres_conf,
            sql_cb_company
        )

        return pd.concat([df_cb_job, df_cb_company], ignore_index=True)
