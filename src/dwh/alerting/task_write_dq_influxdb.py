import datetime
import pandas as pd
from itertools import product
from tqdm import tqdm

from core.app_base import AppBase
from libs.storage_utils import get_flux_client, write_point_influx, \
    get_bq_cli, load_df_from_bq, load_sa_creds, load_df_from_gsheet, load_gsheet_creds


def fill_null_table(df, tags_key, fields_key, time_key):

    miss_from = df.groupby(tags_key)[time_key].max().min()
    miss_to = datetime.datetime.now() - datetime.timedelta(1)

    tags = list()
    for k in tags_key:
        tags.append(set(df[k].tolist()))

    df[time_key] = df[time_key].map(lambda x: x.date())
    df = df.set_index([time_key, *tags_key])
    dtr = pd.date_range(miss_from, miss_to).map(lambda x: x.date())
    tmp = product(dtr, *tags)
    empty_df = pd.DataFrame(index=tmp, columns=fields_key)
    df = pd.concat([df, empty_df[~empty_df.index.isin(df.index)]]).sort_index().fillna(0).reset_index()
    return df


class TaskWriteDqInfluxdb(AppBase):
    def __init__(self, config):
        super(TaskWriteDqInfluxdb, self).__init__(config)        
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
        point = dict()
        point["measurement"] = measurement_name
        point["tags"] = {}
        point["fields"] = {}

        for tag in tags_key:
            point["tags"][tag] = col[tag]

        for field in fields_key:
            point["fields"][field] = col[field]

        my_time = datetime.datetime.min.time()
        my_datetime = datetime.datetime.combine(col['time'], my_time)
        point["time"] = my_datetime
        return point

    def execute(self):

        for _id in self.query_id:
            print("[INFO]: Load query {}-th".format(_id))
            query = self.queries.loc[self.queries["id"] == _id]["query"][_id-1]
            measurement_name = self.queries[self.queries["id"] == _id]["measurement"][_id - 1]
            tags_key = self.queries[self.queries["id"] == _id]["tags_key"][_id - 1].split(',')
            fields_key = self.queries[self.queries["id"] == _id]["fields_key"][_id - 1].split(',')
            time_key = self.queries[self.queries["id"] == _id]["time_key"][_id - 1]
            print(query)

            df = load_df_from_bq(self.sa_creds, query)     
            df = fill_null_table(df, tags_key, fields_key, time_key)

            for i in tqdm(range(len(df))):
                convert_dict = dict()
                convert_dict['time'] = df[time_key][i]
                for key in tags_key:
                    convert_dict[key] = df[key][i]

                for field in fields_key:
                    convert_dict[field] = df[field][i]

                point = self.convert_to_point_flux(measurement_name, convert_dict, tags_key, fields_key)
                write_point_influx(self.flux_cli, self.bucket, point)
