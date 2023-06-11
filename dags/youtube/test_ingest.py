from datetime import datetime, date, timedelta
import sys
import os
# Append the root directory of your project to sys.path
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_dir)
# from data.raw.youtube.ingest_ytb import Ingest_YTB
import plugins.helpers.utils import *


# def testing():
#     import os
#     video_url = 'https://www.youtube.com/shorts/0Vf1TpucUss'
#     src_file_path = "/otp/airflow/0Vf1TpucUss.mp4"
#     ytb_loader = Ingest_YTB('youtube')
#     ytb_loader.execute(video_url, src_file_path)



# if __name__ == '__main__':
#     testing()