import pandas as pd
import re
from dateutil import tz

from libs.storage_utils import load_df_from_postgres
from jobs.backup.task_backup_s3 import TaskBackupS3

local_tz = tz.tzlocal()
remove_non_alphabet = re.compile('[^a-zA-Z ]')


class TaskBackupVnw(TaskBackupS3):

    def extract(self):
        sql_vnw_job = "select 'vnw' as channel_code, 'job' as sourcetype, " \
                      "id, url, created_at from vnw_job " \
                      "where created_at >= '{}' and created_at <= '{}'".format(self.from_date, self.to_date)
        df_vnw_job = load_df_from_postgres(
            self.postgres_conf,
            sql_vnw_job
        )
        sql_vnw_company = "select 'vnw' as channel_code, 'company' as sourcetype, " \
                          "id, name as url, created_at from vnw_company " \
                          "where created_at >= '{}' and created_at <= '{}'".format(self.from_date, self.to_date)
        df_vnw_company = load_df_from_postgres(
            self.postgres_conf,
            sql_vnw_company
        )
        df_vnw_company['url'] = df_vnw_company.apply(
            lambda x: "https://www.vietnamworks.com/viec-lam-tai-{0}-e{1}-vn".format(
                remove_non_alphabet.sub('', x['url'].lower()).replace(' ', '-'),
                x['id']
            ),
            axis=1
        )

        return pd.concat([df_vnw_job, df_vnw_company], ignore_index=True)
