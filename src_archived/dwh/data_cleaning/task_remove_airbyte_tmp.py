
# * https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.TableListItem.html
from core.app_base import AppBase
from google.cloud import bigquery
from datetime import datetime, timedelta
from dateutil import tz
import pandas as pd
import re
import requests


local_tz = tz.tzlocal()
now = datetime.now(local_tz)

class TaskRemoveAirbyteTmp(AppBase):

    def __init__(self, config):
        super(TaskRemoveAirbyteTmp, self).__init__(config)
        self.dp_ops = self.get_param_config(['dp-ops'])
        self.client = bigquery.Client.from_service_account_info(self.dp_ops)
        self.from_date = self.get_process_info(["from_date"])
        self.to_date = self.get_process_info(["to_date"])
        self.days_from_last_modified = self.get_param_config(["days_from_last_modified"])


    def get_airbyte_tmp_table(self):
        tbl_lst = list()
        datasets = list(self.client.list_datasets())
        for dataset in datasets:
            dataset_name = dataset.dataset_id
            if  re.search("^src_", dataset_name) or  re.search("^qc_", dataset_name) or dataset_name == 'stag_erp':
                tables = self.client.list_tables(dataset.dataset_id)  
                for table in tables: 
                    # * just check modified day for airbyte tmp table
                    if '_airbyte' in table.table_id:
                        table_info = self.client.get_table(table.reference)
                        table_last_modified = table_info.modified
                        table_full_id = table_info.full_table_id
                        if table_last_modified < (now - timedelta(days= self.days_from_last_modified)):
                            tbl_lst.append(table_full_id.replace(":","."))
        return tbl_lst

    def delete_table(self,table_id):
        self.client.delete_table(table_id, not_found_ok=True) 

    def delete_airbyte_tmp_table(self):
        del_lst = self.get_airbyte_tmp_table()
        for el in del_lst:
            self.delete_table(el)
        
        message_data = {
                "blocks": [
                        {
                                "type": "divider"
                        },
                        {
                                "type": "section",
                                "text": {
                                        "type": "mrkdwn",
                                        "text": "[ Remove_airbyte_tmp_table ] {} removed".format(len(del_lst))
                                }
                        }
                ]
        }
        webhook_url = "https://hooks.slack.com/services/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"    
        r = requests.post( webhook_url, json= message_data, headers={'Content-Type': 'application/json'})
        
    
        
    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass

    def execute(self):
        self.delete_airbyte_tmp_table()
