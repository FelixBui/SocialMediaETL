from airflow.contrib.hooks.mongo_hook import MongoDBHook

class MyMongoDBHook(MongoDBHook):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
    def get_conn(self):
        return MongoDBHook.get_conn()