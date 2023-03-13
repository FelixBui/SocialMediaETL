import datetime
from dateutil import tz
import io
import tqdm
import zipfile
from zipfile import ZipFile
import pandas as pd
from core.app_base import AppBase
from libs.storage_utils import get_boto3_s3_client, upload_file_to_s3, list_object_from_s3, down_file_from_s3

local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class TaskExtractSalesCall(AppBase):

    def __init__(self, config):
        super(TaskExtractSalesCall, self).__init__(config)
        self.from_date = self.get_process_info(['from_date']) - datetime.timedelta(days=1)
        self.to_date = self.get_process_info(['to_date']) - datetime.timedelta(days=1)
        aws_certs_conf = self.get_param_config(['aws_certs'])
        self.boto_s3_cli = get_boto3_s3_client(aws_certs_conf)
        self.s3_template = self.get_param_config(['s3_template'])

    def extract(self):
        tmp = list()
        for e in list_object_from_s3(self.boto_s3_cli, bucket=self.s3_template['bucket']):
            tmp.append(e)
        df = pd.DataFrame(tmp)

        df = df.loc[df['Key'].map(lambda x: 'sv-call-history' in x)]
        df['call_center'] = df['Key'].map(lambda x: x.split('/')[1])
        df['ds'] = df['Key'].map(lambda x: x.split('/')[2])
        df['file_name'] = df['Key'].map(lambda x: x.split('/')[3])
        df = df.loc[
            (df['ds'] < str(self.to_date.date())) &
            (df['ds'] >= str(self.from_date.date()))
        ]
        return df

    def transform(self, df_raw):
        for _, r in tqdm.tqdm(df_raw.iterrows()):
            key = r['Key']
            ds = r['ds']
            call_center = r['call_center']
            obj = io.BytesIO()

            down_file_from_s3(
                self.boto_s3_cli,
                bucket=self.s3_template['bucket'],
                object_name=key,
                obj=obj,
                use_obj=True,
            )
            obj_key_template = self.s3_template['obj_key_template']
            try:
                with ZipFile(obj) as archive:
                    for f in tqdm.tqdm(archive.filelist):
                        fio = io.BytesIO()
                        with archive.open(f.filename) as fz:
                            fio.write(fz.read())
                            obj_key = obj_key_template.format(call_center, ds, f.filename)
                            fio.seek(0)
                            upload_file_to_s3(
                                self.boto_s3_cli,
                                obj=fio,
                                bucket=self.s3_template['bucket'],
                                object_key=obj_key,
                                use_obj=True)
            except zipfile.BadZipFile as e:
                msg = "{} :: {}".format(str(e), key)
                self.send_message(msg)
            obj.close()

    def execute(self):
        # step 1: extract
        self.log.info("# step 1: extract")
        df_raw = self.extract()

        # step 2: transform
        self.log.info("# step 2: transform")
        self.transform(df_raw)
