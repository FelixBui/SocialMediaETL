from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

# Define the SSH connection details
ssh_conn_id = 'ssh_connection'  # Airflow SSH connection ID
remote_host = 'AIRFLOW_SCHEDULER_IP'  # IP address of airflow-scheduler-7dd8f8b7fb-lwjss
remote_username = 'USERNAME'  # Username for SSH login
remote_password = 'PASSWORD'  # Password for SSH login

# Define the command to execute on airflow-scheduler-7dd8f8b7fb-lwjss
command = 'python /path/to/file.py'  # Path to the Python file on airflow-scheduler-7dd8f8b7fb-lwjss

default_args = {
    "owner": "tmq",
    "retries": 1,
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

with dag:
    start_task = DummyOperator(task_id = "start")
    end_task = DummyOperator(task_id = "end")
    first_task = SSHOperator(
    task_id="execute_remote_python_file",
    ssh_conn_id=ssh_conn_id,
    remote_host=remote_host,
    remote_username=remote_username,
    remote_password=remote_password,
    command=command,
    dag=dag
)

    start_task >> first_task >> end_task
