import os
import argparse
import datetime
import json
from dateutil.parser import parse as parse_time
import sys

DEFAULT_CONFIG = ["db", "telegram", "dp-ops", "mkt-auth"]
PROJECT_DIR = os.environ["PROJECT_DIR"]
KEY_DEFAULT_PATH = os.environ["KEY_DEFAULT_PATH"]
SRC_DIR = os.environ.get(PROJECT_DIR, "src")
# CONFIG_DIR = os.path.join(PROJECT_DIR, "config")
sys.path.insert(0, SRC_DIR)

from core.app_base import AppBase
from libs.import_utils import get_object_by_string
from libs.storage_utils import load_df_from_postgres


def get_class_name(s):
    return ''.join(x for x in s.title() if not x.isspace()).replace('_', '')


def load_config(kw, params):
    conf = {
        "execution_date": kw.execution_date,
        "from_date": kw.from_date,
        "to_date": kw.to_date,
        "process_type": kw.process_type,
        "process_group": kw.process_group,
        'process_name': kw.process_name,
        "params": {}
    }
    for k in DEFAULT_CONFIG:
        default_file = os.path.join(KEY_DEFAULT_PATH, k + ".json")
        conf['params'][k] = json.load(open(default_file))
    for k in params:
        conf['params'][k] = params[k]
    return conf


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a ETL job')
    parser.add_argument("--ptype", type=str, default="etl", dest="process_type",
                        help='type processing: etl, scripts')
    parser.add_argument("--pgroup", type=str, default="centralize", dest="process_group",
                        help='group processing: pricing, centralize, backup, default, ...')
    parser.add_argument('--pname', type=str, required=True, dest='process_name',
                        help="The name of the job module you want to run")
    parser.add_argument('--config', type=str, dest='job_config',
                        help="Extra job config")
    parser.add_argument('--execution_date', type=parse_time, default=datetime.datetime.now(), dest='execution_date',
                        help="Execution date")
    parser.add_argument('--from_date', type=parse_time, default=None, dest='from_date',
                        help="Execution from date")
    parser.add_argument('--to_date', type=parse_time, default=None, dest='to_date',
                        help="Execution to date")
    parser.add_argument('--backfill', default=False, dest='backfill',
                        help="Backfill Data")

    args = parser.parse_args()
    print(args)

    mt_path = os.path.join(KEY_DEFAULT_PATH, "mt_db.json")
    if os.path.exists(mt_path):
        mt_db_conf = json.load(open(mt_path))
        task_df = load_df_from_postgres(
            mt_db_conf,
            query="select * from airflow_task where ptype='{}' and pgroup='{}' and pname='{}'".format(
                args.process_type, args.process_group, args.process_name
            ))
        tmp = {}
        if task_df.shape[0] > 0:
            tmp = json.loads(task_df['config'].iloc[0].replace('\\\\', '\\'))
    else:
        job_conf_path = os.path.join(
            PROJECT_DIR, "config", "jobs", args.process_group, args.process_name + ".json")
        tmp = json.load(open(job_conf_path))

    config = load_config(args, tmp)

    if config['process_type'] == "etl":
        module_name = f"jobs.{config['process_group']}.{config['process_name']}"
    elif config['process_type'] == "script":
        module_name = "scripts.%s" % config['process_name']
    elif config['process_type'] == "crawler":
        module_name = "crawler.%s" % config['process_name']
    elif config['process_type'] == "dwh":
        module_name = f"dwh.{config['process_group']}.{config['process_name']}"
    elif config['process_type'] == "services":
        module_name = f"services.{config['process_group']}.{config['process_name']}"
    else:
        raise ValueError("process type: {} not support".format(config['process_type']))
    class_name = get_class_name(module_name.split('.')[-1])
    task: AppBase = get_object_by_string(module_name, class_name)(config)

    if args.backfill:
        task.backfill()
    else:
        task.execute()
