from google.cloud import bigquery
import matplotlib.pyplot as plt
import random
import numpy as np
import pandas as pd
import datetime
def query(content_query):
    bq_cli = bigquery.Client()
    job = bq_cli.query(content_query)
    return job.to_dataframe()

content_query = """with result as( SELECT transaction_dt, tbl_grp, table_name, sum(val) as value FROM `data-warehouse-sv.dwh_prod.net_sales_alert` group by 1,2,3 union all SELECT transaction_dt, tbl_grp, table_name, sum(val) as value FROM `data-warehouse-sv.dwh_prod.revenue_alert` group by 1,2,3 ) select * from result"""
data = query(content_query)

tags_key = ['tbl_grp','table_name']
fields_key = ['value']

def fill_null_table(df, tags_key, fields_key, time_key):

    miss_from = df.groupby(tags_key)[time_key].max().min()
    miss_to = datetime.now()

    tags = list()
    for k in tags_key:
        tags.append(set(df[k].tolist()))
    
    df = df.set_index([time_key, *tags_key])
    dtr = pd.date_range(miss_from, miss_to).map(lambda x: x.date())
    tmp = product(dtr, *tags)
    empty_df = pd.DataFrame(index=tmp, columns=fields_key)
    df = pd.concat([df, empty_df[~empty_df.index.isin(df.index)]]).sort_index().fillna(0).reset_index()
    return df

sales_df = data[data['table_name']== 'dwh_fact_sales_order_items']
date_min = sales_df['transaction_dt'].min()
date_max = sales_df['transaction_dt'].max()
def find_outlier(df, q=0.25):
    upper = df.quantile(1-q)
    lower = df.quantile(q)
    return upper, lower

def find_outlier(df_in, col_name):
    q1 = df_in[col_name].quantile(0.25)
    q3 = df_in[col_name].quantile(0.75)
    iqr = q3-q1 #Interquartile range
    fence_low  = q1-1.5*iqr
    fence_high = q3+1.5*iqr

    return fence_low, fence_high

rs = list()
for dt in pd.date_range(date_min, date_max):
    # print(dt, dt - datetime.timedelta(14))
    in_df= sales_df[(sales_df['transaction_dt'] < dt) &   (sales_df['transaction_dt'] > (dt - datetime.timedelta(14)))]
    l, u = find_outlier(in_df, "value")
    rs.append((dt, *sales_df[sales_df['transaction_dt'] == dt]["tbl_grp"], *sales_df[sales_df['transaction_dt'] == dt]["table_name"], l, u, *sales_df[sales_df['transaction_dt'] == dt]["value"]))
df = pd.DataFrame(rs, columns=['dt','tbl_grp','table_name','l', 'u','v'])
df.fillna(0)

for i in range(len(df)):
    if df['l'][i] < 0:
        df['l'][i] = 0 
    if df['u'][i] < 0:
        df['u'][i] = 0

from datetime import datetime, timedelta
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd

class InfluxClient:
    def __init__(self,url,token,org,bucket): 
        self._org=org 
        self._bucket = bucket
        self._client = InfluxDBClient(url=url, token=token, org=org)

        # from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

    def write_data(self,data,write_option=SYNCHRONOUS):
        write_api = self._client.write_api(write_option)
        write_api.write(self._bucket, self._org , data, write_precision='s')

bucket = "try"
token = "wNbxqXachC2RfoLwKqKQ_qYMUOISVvjrwaswXcQ2lg2m__fOgNjoNA6qm7QuGm2YM2DUioj9ysZWFnz_b-RQMw=="
org = "primary"#"nhanlucsieuviet"
url="https://influxdb.dp.sieuviet.services/"#"http://localhost:8086"
IC = InfluxClient(url,token,org,bucket)

tags_key = ['tbl_grp','table_name']
fields_key = ['l','u','v']

for idx in range(len(df)):
    point = {}
    point["measurement"]= 'test'
    point["tags"] = {}
    point["fields"] = {}

    for tag in tags_key:
        point["tags"][tag] = df[tag][idx]

    for field in fields_key:
        point["fields"][field] = df[field][idx]

    my_time = datetime.min.time()
    my_datetime = datetime.combine(df['dt'][idx], my_time)
    point["time"] = my_datetime
    IC.write_data(point)
    