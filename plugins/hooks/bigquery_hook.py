from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

class MyBigQueryHook(BigQueryHook):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
    def get_conn(self):
        return BigQueryHook.get_conn()
