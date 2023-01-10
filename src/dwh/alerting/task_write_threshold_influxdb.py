from datetime import datetime
import pandas as pd
from itertools import product
from tqdm import tqdm
from influxdb_client import Point


from core.app_base import AppBase
from libs.storage_utils import get_flux_client, write_point_influx, \
    get_bq_cli, load_sa_creds, load_df_from_gsheet, load_gsheet_creds


class TaskWriteThresholdInfluxdb(AppBase):
    def __init__(self, config):
        super(TaskWriteThresholdInfluxdb, self).__init__(config)        
        influx_conf = self.get_param_config(['db', 'influx-prod'])
        dp_ops = self.get_param_config(["dp-ops"])
        self.bq_cli = get_bq_cli(dp_ops)
        self.sa_creds = load_sa_creds(dp_ops)
        self.gsheet_creds = load_gsheet_creds(dp_ops)
        self.sheet_id = self.get_param_config(["sheet", 'sheet_id'])
        self.sheet_name = self.get_param_config(["sheet", 'sheet_name'])
        self.queries = load_df_from_gsheet(self.gsheet_creds, self.sheet_id, self.sheet_name)
        self.query_id = self.get_param_config(["query_id"])       
        self.bucket = self.get_param_config(['bucket'])  
        self.flux_cli = get_flux_client(influx_conf)

    @staticmethod
    def convert_to_point_flux(measurement_name, col, tags_key, fields_key): 
        point = {}
        point["measurement"]= measurement_name
        point["tags"] = {}
        point["fields"] = {}

        for tag in tags_key:
            point["tags"][tag] = col[tag]

        for field in fields_key:
            point["fields"][field] = float(col[field])

        # date = datetime.now().strftime("%Y-%m-%d")
        # my_time = datetime.min.time()
        # my_datetime = datetime.combine(date, my_time)
        my_datetime = datetime.now()
        point["time"] = my_datetime
        return point

    def execute(self):

        for id in self.query_id:
            print("[INFO]: Load query {}-th".format(id))
            # query = self.queries.loc[self.queries["id"] == id]["query"][id-1]
            measurement_name = self.queries[self.queries["id"] == id]["measurement"][id - 1]  
            table_name = self.queries[self.queries["id"] == id]["table_name"][id - 1]  
            column = self.queries[self.queries["id"] == id]["column"][id - 1] 
            channel_code = self.queries[self.queries["id"] == id]["channel_code"][id - 1] 
            threshold_from = self.queries[self.queries["id"] == id]["threshold_from"][id - 1] 
            threshold_to = self.queries[self.queries["id"] == id]["threshold_to"][id - 1]

            point = (
                Point(measurement_name)
                .tag("table_name", table_name)
                .tag("column", column)
                .tag("channel_code", channel_code)
                .field("threshold_from", float(threshold_from))
                .field("threshold_to", float(threshold_to))
                .time(int(datetime.strptime(datetime.now().strftime("%Y-%m-%d"),'%Y-%m-%d').timestamp())*1000000000)
            )
            # print(point)
            # input()
            write_point_influx(self.flux_cli, self.bucket , point)
            # tags_key =  self.queries[self.queries["id"] == id]["tags_key"][id - 1].split(',') 
            # fields_key = self.queries[self.queries["id"] == id]["fields_key"][id - 1].split(',')
            # time_key = self.queries[self.queries["id"] == id]["time_key"][id - 1]

            # convert_dict = {}
            # for key in tags_key:
            #     convert_dict[key] = self.queries[key][id - 1]

            # for field in fields_key:
            #     convert_dict[field] = self.queries[field][id - 1]

            # point = self.convert_to_point_flux(measurement_name, convert_dict, tags_key, fields_key)
            # print(point)
            # input()
            # write_point_influx(self.flux_cli, self.bucket , point)
            

            # df = load_df_from_bq(self.sa_creds, query)            
            # df = fill_null_table(df, tags_key, fields_key, time_key)

            # for i in tqdm(range(len(df))):
            #     convert_dict = {}
            #     convert_dict['time'] = df[time_key][i]
            #     for key in tags_key:
            #         convert_dict[key] = df[key][i]

            #     for field in fields_key:
            #         convert_dict[field] = df[field][i]

                # point = self.convert_to_point_flux(measurement_name, convert_dict, tags_key, fields_key)
                # write_point_influx(self.flux_cli, self.bucket , point)
