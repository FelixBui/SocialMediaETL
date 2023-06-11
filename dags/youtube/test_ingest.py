from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator



def testing():
    import os
    video_url = 'https://www.youtube.com/shorts/0Vf1TpucUss'
    src_file_path = "/otp/airflow/0Vf1TpucUss.mp4"
    ytb_loader = Ingest_YTB('youtube')
    ytb_loader.execute(video_url, src_file_path)

def test_import():
    from plugins.helpers.utils import get_gcp_creds
    return get_gcp_creds()

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
    dag_id="ytb.test_import",
    default_args=default_args,
    schedule_interval = "@once",
    start_date=datetime(2023, 1, 3),
    catchup=False,
    tags=["testing"]
) 

with dag:
    start_task = DummyOperator(task_id = "start")
    end_task = DummyOperator(task_id = "end")

    test_import_task = PythonOperator(task_id = "hello_word", python_callable=test_import())
    start_task >> test_import_task >> end_task

