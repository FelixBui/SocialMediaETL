import pandas as pd
import re
from dateutil import tz

from libs.storage_utils import load_df_from_postgres
from jobs.backup.task_backup_s3 import TaskBackupS3

local_tz = tz.tzlocal()
remove_non_alphabet = re.compile('[^a-zA-Z ]')


class TaskBackupTopcv(TaskBackupS3):

    def extract(self):
        sql_topcv_job = "select 'topcv' as channel_code, 'job' as sourcetype, " \
                        "id, url, created_at from topcv_job " \
                        "where created_at >= '{}' and created_at <= '{}'".format(self.from_date, self.to_date)
        df_topcv_job = load_df_from_postgres(
            self.postgres_conf,
            sql_topcv_job
        )
        sql_topcv_company = "select 'topcv' as channel_code, 'company' as sourcetype, " \
                            "id, url, created_at from topcv_company " \
                            "where created_at >= '{}' and created_at <= '{}'".format(self.from_date, self.to_date)
        df_topcv_company = load_df_from_postgres(
            self.postgres_conf,
            sql_topcv_company
        )

        return pd.concat([df_topcv_job, df_topcv_company], ignore_index=True)
