# import re
# from abc import ABC

# from google.cloud import storage, bigquery
# import pymysql.cursors
# import io
# import pandas as pd
# from sqlalchemy import create_engine
# import tqdm
from abc import ABC

from core.app_base import AppBase


class ReportBase(AppBase, ABC):
    def __init__(self, config):
        super(ReportBase, self).__init__(config)
