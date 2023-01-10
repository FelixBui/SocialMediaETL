import datetime
import functools
from urllib.parse import quote_plus
import pika
import json
import time
import random

import pandas as pd
import psycopg2
import pymongo
import pymysql
import tqdm
from bson.codec_options import CodecOptions
from dateutil import tz
from pandas import ExcelWriter
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy import types
# from neo4j import GraphDatabase
import boto3
import logging
from botocore.exceptions import ClientError
from google.cloud import bigquery, storage
from google.oauth2.service_account import Credentials
import gspread
import io
import uuid
import gcsfs

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


local_tz = tz.tzlocal()
utc_tz = tz.tzutc()
codec_options = CodecOptions(tz_aware=True, tzinfo=local_tz)


def merge_dfs(dfs, on_lst, how="outer"):
    return functools.reduce(lambda l, r: pd.merge(l, r, on=on_lst, how=how), dfs)


def count_na(s: pd.Series):
    return len(s[s.isna()])


def count_distinct(s: pd.Series):
    return len(set(s))


def split_dataframe(df, chunk_size=5000):
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        yield df.iloc[(i * chunk_size): (i + 1) * chunk_size]


def get_postgres_engine(postgres_conf, mode='write'):
    t = postgres_conf.get("type", "postgres")
    if t == 'postgres-repl':
        if mode == 'write':
            host = postgres_conf["host"]["master"]
        else:
            host = postgres_conf["host"]["slave"]
    else:
        host = postgres_conf["host"]
    engine = create_engine(
        "postgresql://{user}:{pw}@{host}:{port}/{db}".format(
            user=postgres_conf["user"],
            pw=postgres_conf["password"],
            host=host,
            port=postgres_conf["port"],
            db=postgres_conf["database"],
        )
    )
    return engine


def get_postgres_cli(postgres_conf, mode='write'):
    t = postgres_conf.get("type", "postgres")
    if t == 'postgres-repl':
        if mode == 'write':
            host = postgres_conf["host"]["master"]
        else:
            host = postgres_conf["host"]["slave"]
    else:
        host = postgres_conf["host"]
    postgres_cli = psycopg2.connect(
        database=postgres_conf["database"],
        user=postgres_conf["user"],
        password=postgres_conf["password"],
        host=host,
        port=postgres_conf["port"],
    )

    # with postgres_cli.cursor() as cur:
    #     print('PostgreSQL database version:')
    #     cur.execute('SELECT version()')
    #     db_version = cur.fetchone()
    #     print(db_version)

    return postgres_cli


def load_df_from_postgres(postgres_conf, query):
    engine = get_postgres_engine(postgres_conf)
    df = pd.read_sql(query, engine)
    return df


def insert_df_to_postgres_fast(postgres_conf, tbl_name, df, mode):
    if df.shape[0] == 0:
        return

    engine = get_postgres_engine(postgres_conf)
    if mode == "replace":
        dct = df.iloc[0].to_dict()
        dct_type = dict()
        for k, v in dct.items():
            if isinstance(v, (list, type)):
                dct_type[k] = postgresql.ARRAY(types.String)

        df.to_sql(tbl_name, engine, if_exists="replace", index=False, dtype=dct_type)
    else:
        raise Exception("mode {0} not support".format(mode))


def __insert_df_to_postgres(postgres_conf, tbl_name, df, primary_keys: list, schema="public"):
    if df.shape[0] < 1:
        return

    def norm_value(d: dict):
        for k, v in d.items():
            if isinstance(v, (list, tuple)):
                continue
            if pd.isnull(v):
                d[k] = None
        return d

    df = df.applymap(lambda x: x.replace(chr(0), '') if isinstance(x, str) else x)

    engine = get_postgres_engine(postgres_conf)
    meta = MetaData(schema=schema)

    conn = engine.connect()
    my_table = Table(tbl_name, meta, autoload=True, autoload_with=conn)
    insert_statement = postgresql.insert(my_table).values(
        list(map(norm_value, df.to_dict(orient="records")))
    )
    if len(primary_keys):
        set_ = {
             c.key: c for c in insert_statement.excluded if c.key not in primary_keys
         }
        if len(set_) > 0:
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=primary_keys,
                set_=set_
            )
        else:
            upsert_statement = insert_statement.on_conflict_do_nothing(
                index_elements=primary_keys,
            )

        conn.execute(upsert_statement)
    else:
        conn.execute(insert_statement)


def insert_df_to_postgres(postgres_conf, tbl_name, df, primary_keys: list, schema="public", chunk_size=5000):
    with tqdm.tqdm(total=df.shape[0]) as pbar:
        for _df in split_dataframe(df, chunk_size=chunk_size):
            if isinstance(primary_keys, list) and len(primary_keys) > 0:
                __insert_df_to_postgres(
                    postgres_conf, tbl_name, _df.drop_duplicates(primary_keys), primary_keys, schema)
            else:
                __insert_df_to_postgres(
                    postgres_conf, tbl_name, _df, primary_keys, schema)
            pbar.update(_df.shape[0])


def insert_big_df_to_postgres(postgres_conf, tbl_name, df, primary_keys: list, schema="public", chunk_size=10000):
    with tqdm.tqdm(total=df.shape[0]) as pbar:
        for _df in split_dataframe(df, chunk_size=chunk_size):
            insert_df_to_postgres(postgres_conf, tbl_name, _df, primary_keys, schema)
            pbar.update(_df.shape[0])


def save_xls(lst_dfs, xls_path, lst_sheet_name=None):
    with ExcelWriter(xls_path) as writer:
        for n, df in enumerate(lst_dfs):
            try:
                sheet_name = lst_sheet_name[n]
            except Exception as e:
                print(e)
                sheet_name = "sheet_%s" % n

            df.to_excel(writer, sheet_name)
        writer.save()


def get_mongo_cli(mongo_conf):
    if mongo_conf.get('uri') is not None:
        uri = mongo_conf['uri']
    elif mongo_conf['user']:
        uri = "mongodb://%s:%s@%s:%s" % (
            quote_plus(mongo_conf["user"]),
            quote_plus(mongo_conf["password"]),
            mongo_conf["host"],
            mongo_conf["port"],
        )
    else:
        uri = "mongodb://%s:%s" % (mongo_conf["host"],
                                   mongo_conf["port"])

    client = pymongo.MongoClient(uri)
    return client


def insert_df_into_mongodb(
        mongo_conf, collection_name, df, primary_key="id", upsert=True, batch_size=500):
    if df.shape[0] < 1:
        return

    mongo_cli = get_mongo_cli(mongo_conf)
    df["updated_at"] = datetime.datetime.now(tz=local_tz)
    if (primary_key is None) or (len(primary_key) == 0):
        mongo_cli[mongo_conf["database"]][collection_name].insert_many(
            df.to_dict(orient="records")
        )
        return
    objects = df.to_dict(orient="records")

    if isinstance(primary_key, str):
        update_objects = list()
        with tqdm.tqdm(total=len(objects)) as pbar:
            for obj in objects:
                update_objects.append(
                    pymongo.UpdateOne(
                        {primary_key: obj[primary_key]}, {"$set": obj}, upsert=upsert
                    )
                )
                if len(update_objects) > batch_size:
                    # mongo_result =
                    mongo_cli[mongo_conf["database"]][collection_name].bulk_write(
                        update_objects
                    )
                    # print(mongo_result.bulk_api_result)
                    update_objects = list()
                pbar.update(1)
            if len(update_objects) > 0:
                mongo_cli[mongo_conf["database"]][collection_name].bulk_write(
                    update_objects
                )
                # print(mongo_result.bulk_api_result)
                del update_objects
    else:
        assert isinstance(
            primary_key, list
        ), "primary key must be string or list of string, primary key: {}".format(
            primary_key
        )
        update_objects = list()
        with tqdm.tqdm(total=len(objects)) as pbar:
            for obj in objects:
                update_objects.append(
                    pymongo.UpdateOne(
                        {k: obj[k] for k in primary_key}, {"$set": obj}, upsert=upsert
                    )
                )
                if len(update_objects) > batch_size:
                    mongo_cli[mongo_conf["database"]][collection_name].bulk_write(
                        update_objects
                    )
                    # print(mongo_result.bulk_api_result)
                    update_objects = list()
                pbar.update(1)
            if len(update_objects) > 0:
                mongo_cli[mongo_conf["database"]][collection_name].bulk_write(
                    update_objects
                )
                # print(mongo_result.bulk_api_result)
                del update_objects


def load_df_from_mongo(
        mongo_conf, collection_name, query, no_id=True,
        selected_keys=None, process_bar=False):
    mongo_cli = get_mongo_cli(mongo_conf)
    db = mongo_cli[mongo_conf["database"]].with_options(codec_options=codec_options)
    if selected_keys is None:
        cursor = db[collection_name].find(query)
    else:
        cursor = db[collection_name].find(query, selected_keys)
    if process_bar:
        rs = list()
        total = db[collection_name].count_documents(query)
        for e in tqdm.tqdm(cursor, total=total):
            rs.append(e)
        df = pd.DataFrame(rs)
    else:
        df = pd.DataFrame(list(cursor))

    if df.shape[0] == 0:
        return df
    # Delete the _id
    if no_id:
        del df["_id"]
    return df


def yield_mongo_rows(cursor, chunk_size):
    """
    Generator to yield chunks from cursor
    :param cursor:
    :param chunk_size:
    :return:
    """
    chunk = []
    for i, row in enumerate(cursor):
        if i % chunk_size == 0 and i > 0:
            yield chunk
            del chunk[:]
        chunk.append(row)
    yield chunk


def yield_df_load_from_mongo(mongo_conf, collection_name, query, no_id=True, selected_keys=None, batch_size=10000):
    """
    Generator to yield chunks from cursor
    :param mongo_conf: dictionary mongo config
    :param collection_name: collection name
    :param query: query
    :param no_id: no _id
    :param selected_keys:
    :param batch_size: return max chunk_size rows
    :return:
    """
    mongo_cli = get_mongo_cli(mongo_conf)
    db = mongo_cli[mongo_conf["database"]].with_options(codec_options=codec_options)
    if selected_keys is None:
        cursor = db[collection_name].find(query, batch_size=batch_size)
    else:
        cursor = db[collection_name].find(query, selected_keys, batch_size=batch_size)

    chunks = yield_mongo_rows(cursor, batch_size)
    for chunk in chunks:
        df = pd.DataFrame(chunk)
        if df.shape[0] == 0:
            yield df
        # Delete the _id
        if no_id:
            del df["_id"]
        yield df


def get_boto3_s3_client(aws_conf):
    return boto3.client(
        's3',
        aws_access_key_id=aws_conf["aws_access_key_id"],
        aws_secret_access_key=aws_conf["aws_secret_access_key"]
    )


def upload_file_to_s3(boto3_client, obj, bucket, object_key, use_obj=False):
    """Upload a file to an S3 bucket

    :param boto3_client: s3 client
    :param obj: File to upload
    :param bucket: Bucket to upload to
    :param object_key: S3 object name. If not specified then file_name is used
    :param use_obj
    :return: True if file was uploaded, else False
    """

    # Upload the file
    try:
        if not use_obj:
            boto3_client.upload_file(obj, bucket, object_key)
        else:
            boto3_client.upload_fileobj(obj, bucket, object_key)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def down_file_from_s3(boto3_client, bucket, object_name, obj=None, use_obj=False):
    """Upload a file to an S3 bucket

    :param boto3_client: s3 client
    :param obj: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :param use_obj
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = obj

    # Down the file
    try:
        if not use_obj:
            boto3_client.download_file(bucket, object_name, obj)
        else:
            boto3_client.download_fileobj(bucket, object_name, obj)
    except ClientError as e:
        logging.error(e)
        return False
    return True


# def list_object_from_s3(boto3_client, bucket, prefix=''):
#     return boto3_client.list_objects(Bucket=bucket, Prefix=prefix)['Contents']

def list_object_from_s3(boto3_client, bucket, prefix=''):
    paginator = boto3_client.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    for page in page_iterator:
        for e in page['Contents']:
            yield e


def load_df_from_bq(bq_creds, query, project_id='data-warehouse-sv'):
    return pd.read_gbq(query, credentials=bq_creds, project_id=project_id)


def insert_df_to_bq(bq_conf, df, tbl_name, mode="append", primary_keys=None):
    bq_creds = load_sa_creds(bq_conf)
    if mode in ("append", "replace"):
        df.to_gbq(tbl_name, if_exists=mode, credentials=bq_creds)
    elif mode == "upsert":
        if primary_keys is None:
            raise Exception("mode {0} require primary keys".format(mode))
        tbl_tmp = tbl_name + uuid.uuid4().hex[:5]
        bq_cli = get_bq_cli(bq_conf)
        df.to_gbq(tbl_tmp, if_exists="replace", credentials=bq_creds)
        sql_on = " and ".join(map(lambda x: "T.{0} = S.{0}".format(x), primary_keys))
        value_cols = set(df.columns) - set(primary_keys)
        sql_update = ", ".join(map(lambda x: "{0} = S.{0}".format(x), value_cols))
        sql_insert = ", ".join(df.columns)
        sql = """
        MERGE {0} T
        USING {1} S
        ON {2}
        WHEN MATCHED THEN
          UPDATE SET {3}
        WHEN NOT MATCHED THEN
          INSERT ({4}) VALUES({4})
        """.format(tbl_name, tbl_tmp, sql_on, sql_update, sql_insert)
        query_job = bq_cli.query(query=sql)
        query_job.result()
        sql_drop_tmp = "DROP TABLE {}".format(tbl_tmp)
        print(sql_drop_tmp)
        query_job = bq_cli.query(query=sql_drop_tmp)
        r = query_job.result()
        print(list(r))
    else:
        raise Exception("mode {0} not support".format(mode))


# def get_neo_client(neo_conf):
#     uri = "bolt://{0}:{1}".format(neo_conf['host'], neo_conf['port'])
#     auth = (neo_conf['user'], neo_conf['password'])
#     return GraphDatabase.driver(uri, auth=auth, max_connection_lifetime=200)


def get_rmq_cli(rmq_conf):
    user = rmq_conf['user']
    password = rmq_conf['password']
    creds = pika.credentials.PlainCredentials(username=user, password=password)
    rmq_cli = pika.BlockingConnection(
        pika.ConnectionParameters(host=rmq_conf.get('host', 'localhost'), credentials=creds))
    return rmq_cli


def get_rmq_channel(rmq_cli, queue_name):
    rmq_channel = rmq_cli.channel()
    rmq_channel.queue_declare(queue=queue_name, durable=True)
    return rmq_channel


def push_rmq_channel(rmq_channel, routing_key, item, exchange=''):
    rmq_channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=json.dumps(item),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        )
    )


def consume_rmq_channel(rmq_channel, channel_name, inactivity_timeout=20):
    rmq_channel.basic_qos(prefetch_count=30)
    for method_frame, properties, body in rmq_channel.consume(channel_name, inactivity_timeout=inactivity_timeout):
        if body is None:
            break
        msg = json.loads(body)
        yield msg
        rmq_channel.basic_ack(method_frame.delivery_tag)
        num_sleep = random.uniform(0.001, 0.005)
        time.sleep(num_sleep)


# def load_df_from_gsheet(link, select_tab=0):
#     link = "/".join(link.split("/")[:-1])
#     url = "{0}/export?gid={1}&format=csv".format(link, select_tab)
#     return pd.read_csv(url)


def copy_collection(mongo_cli, db_name, col_src, col_target):
    db = mongo_cli[db_name]
    pipeline = [{"$match": {}}, {"$out": col_target}]
    db[col_src].aggregate(pipeline)


def get_mysql_engine(mysql_conf):
    engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(
            user=mysql_conf["user"],
            pw=mysql_conf["password"],
            host=mysql_conf["host"],
            port=mysql_conf["port"],
            db=mysql_conf["database"],
        )
    )
    return engine


def load_df_from_mysql(mysql_conf, query, db_name=None):
    if db_name is not None:
        mysql_conf['database'] = db_name
    engine = get_mysql_engine(mysql_conf)
    df = pd.read_sql(query, con=engine)
    return df


def load_gsheet_creds(account_info):
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_info(
        account_info,
        scopes=scopes
    )
    return creds


def load_df_from_gsheet(creds, sheet_id, worksheet_name):
    sheet_gc = gspread.authorize(creds)
    sh = sheet_gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)
    df = pd.DataFrame(ws.get_all_records())
    return df


def dt_cast_str(x):
    if isinstance(x, (datetime.datetime, datetime.date)):
        return str(x)
    if pd.isna(x):
        return ''
    else:
        return x


def insert_df_to_gsheet(creds, sheet_id, worksheet_name, df):
    sheet_gc = gspread.authorize(creds)
    sh = sheet_gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)
    ws.clear()
    df = df.convert_dtypes()
    for col in df.columns:
        df[col] = df[col].map(dt_cast_str)

    ws.update([df.columns.values.tolist()] + df.values.tolist())


def load_sa_creds(key_path, scopes=None):
    if isinstance(key_path, dict):
        return Credentials.from_service_account_info(key_path, scopes=scopes)
    else:
        return Credentials.from_service_account_file(key_path, scopes=scopes)


def get_gcs_cli(sa_info):
    creds = load_sa_creds(sa_info)
    return storage.Client(credentials=creds, project=creds.project_id)


def get_bq_cli(sa_info):
    creds = load_sa_creds(sa_info)
    return bigquery.Client(credentials=creds, project=creds.project_id)


def upload_df_to_gcs(gcs_cli, df, bucket_name, gcs_path, chunk_size=262144, fmt='parquet'):
    iob = io.BytesIO()
    if fmt == 'parquet':
        df.to_parquet(iob, engine="pyarrow")
    else:
        raise TypeError("format {} not supported".format(fmt))
    bucket = gcs_cli.get_bucket(bucket_name)
    blob = bucket.blob(gcs_path, chunk_size=chunk_size)
    iob.seek(0)
    blob.upload_from_file(iob)


def load_df_from_gcs_folder(sa_info, project_id, bucket_name, gcs_path, fmt='parquet'):
    """
        This function use for loading all file in folder in gcs bucket
        Input folder path -> output dataframe of all file
        This version only use with parquet format
    """
    if fmt == 'parquet':
        pass
    else:
        raise TypeError("format {} not supported".format(fmt))
    fs = gcsfs.GCSFileSystem(project=project_id, token=sa_info)
    file_names = fs.ls('{}/{}'.format(bucket_name, gcs_path))
    df_lst = list()
    for name in file_names:
        with fs.open("gs://{}".format(name)) as f:
            gcs_df = pd.read_parquet(f)
            df_lst.append(gcs_df)
    rs_df = pd.concat(df_lst)
    return rs_df


def down_gcs_to_df(gcs_cli, bucket_name, gcs_path, chunk_size=262144, fmt='parquet'):
    iob = io.BytesIO()
    if fmt == 'parquet':
        pass
        # df.to_parquet(iob, engine="pyarrow")
    else:
        raise TypeError("format {} not supported".format(fmt))
    bucket = gcs_cli.get_bucket(bucket_name)
    blob = bucket.blob(gcs_path, chunk_size=chunk_size)
    contents = blob.download_as_string()
    iob.write(contents)
    iob.seek(0)
    df = pd.read_parquet(iob)
    return df


def load_df_from_gcs_file(sa_info, project_id, bucket_name, gcs_path, fmt='parquet'):
    """
        This function use for loading file gcs bucket
        Input file path -> output dataframe of file's data
        This version only use with parquet format
    """
    if fmt == 'parquet':
        pass
    else:
        raise TypeError("format {} not supported".format(fmt))
    
    fs = gcsfs.GCSFileSystem(project=project_id, token=sa_info)
    with fs.open("gs://{}.pq".format(bucket_name + '/' + gcs_path)) as f:
        gcs_df = pd.read_parquet(f)
        rs_df = gcs_df
    return rs_df

def get_flux_client(conf):
    return InfluxDBClient(url=conf['url'], token=conf['token'], org=conf['org'])


def write_point_influx(flux_cli, bucket, point):
    with flux_cli.write_api(write_options=SYNCHRONOUS) as wapi:
        wapi.write(bucket=bucket, record=point)

