import pandas as pd
import re
from dateutil import tz

from libs.storage_utils import load_df_from_postgres
from jobs.backup.task_backup_s3 import TaskBackupS3

local_tz = tz.tzlocal()
remove_non_alphabet = re.compile('[^a-zA-Z ]')


class TaskBackupItv(TaskBackupS3):

    def extract(self):
        sql_itv_job = "select 'itv' as channel_code, 'job' as sourcetype, id, url, created_at from itv_job " \
                      "where created_at >= '{}' and created_at <= '{}'".format(self.from_date, self.to_date)
        df_itv_job = load_df_from_postgres(
            self.postgres_conf,
            sql_itv_job
        )
        sql_itv_company = "select 'itv' as channel_code, 'company' as sourcetype, " \
                          "id, url, created_at from itv_company " \
                          "where created_at >= '{}' and created_at <= '{}'".format(self.from_date, self.to_date)
        df_itv_company = load_df_from_postgres(
            self.postgres_conf,
            sql_itv_company
        )

        return pd.concat([df_itv_job, df_itv_company], ignore_index=True)
