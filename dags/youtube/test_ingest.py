from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from data.raw.youtube.ingest_ytb import Ingest_YTB



def testing():
    import os
    video_url = 'https://www.youtube.com/shorts/0Vf1TpucUss'
    src_file_path = "/Users/macos/Downloads/0Vf1TpucUss.mp4"
    ytb_loader = Ingest_YTB('youtube')
    ytb_loader.execute(video_url, src_file_path)

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
    dag_id="youtube.hellword",
    default_args=default_args,
    description= 'My first dag',
    schedule_interval = "@once",
    start_date=datetime(2023, 1, 3),
    catchup=False,
    tags=["testing"]
) 

with dag:
    start_task = DummyOperator(task_id = "start")
    end_task = DummyOperator(task_id = "end")

    first_task = PythonOperator(task_id = "hello_word", python_callable=hello_word)
    start_task >> first_task >> end_task

