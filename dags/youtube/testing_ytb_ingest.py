import os
from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from data.raw.youtube.ingest_ytb import Ingest_YTB
from plugins.helpers.utils import clear_mp4_files

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']

def testing():
    video_url = 'https://www.youtube.com/shorts/0Vf1TpucUss'
    src_file_path = f"{AIRFLOW_HOME}/{os.path.basename(video_url)}.mp4"
    ytb_loader = Ingest_YTB('youtube')
    ytb_loader.execute(video_url, src_file_path)
    clear_mp4_files()

default_args = {
    "owner": "longtp",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
    "email": ['philongqh@gmail.com'] ,
    "sla": timedelta(hours=1),
    'email_on_failure': False,
    'email_on_retry': False,
}
dag = DAG(
    dag_id="ytb.test_ingest",
    default_args=default_args,
    schedule_interval = "@once",
    start_date=datetime(2023, 1, 3),
    catchup=False,
    tags=["testing"]
) 

with dag:
    start_task = DummyOperator(task_id = "start")
    end_task = DummyOperator(task_id = "end")

    test_ytb_ingest = PythonOperator(task_id = "hello_word", python_callable=testing)
    start_task >> test_ytb_ingest >> end_task

