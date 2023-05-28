from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.models import Variable

default_args = {
    "owner": "tmq",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
    "email": ['shenkedokato@gmail.com'] ,
    "sla": timedelta(hours=1),
    'email_on_failure': False,
    'email_on_retry': False,
}
dag = DAG(
    dag_id="download_video_yt",
    default_args=default_args,
    description= 'My first dag',
    schedule_interval = "@daily",
    start_date=datetime(2023, 1, 3),
    catchup=False,
    tags=["testing"]
) 

def download_youtube_video():
    video_id= '1XoICkGxWtw'
    video_url = f'https://www.youtube.com/watch?v={video_id}'
    file_name= f'{video_id}.mp4'
    video_path=f'/opt/airflow'
    yt = YouTube(video_url).streams.get_by_itag(22).download(output_path=video_path,filename=file_name)


with dag:
    start_task = DummyOperator(task_id = "start")
    end_task = DummyOperator(task_id = "end")
    first_task = PythonVirtualenvOperator(task_id = "download_video",requirements="pytube==15.0.0", python_callable=download_youtube_video)
    start_task >> first_task >> end_task






    




