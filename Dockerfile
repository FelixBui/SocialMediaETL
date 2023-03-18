FROM python:3.9.12

RUN apt-get update && \
    apt-get install -yqq --no-install-recommends \
    freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    build-essential \
    && pip install --upgrade pip \
    && pip install apache-airflow[gcp]

# Copy the DAGs folder
COPY dags /usr/local/airflow/dags

# Copy the airflow.cfg file
COPY airflow.cfg /usr/local/airflow/airflow.cfg

# Set the environment variables
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://<user>:<password>@<host>:<port>/<database>
ENV AIRFLOW__CORE__FERNET_KEY=<fernet_key>
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
ENV AIRFLOW__CORE__LOGGING_LEVEL=INFO
ENV AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT="google-cloud-platform://?extra__google_cloud_platform__project=<project_id>"

# Expose the Airflow webserver and scheduler ports
EXPOSE 8080 8793

CMD ["airflow", "webserver"]
