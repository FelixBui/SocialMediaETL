import datetime
import time
import pandas as pd
import sys
import pickle
import os
import io
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request
from google.cloud import storage
from google.oauth2 import service_account   
from googleapiclient import errors

def Create_Service(client_secret_file, api_name, api_version, *scopes):
    print(client_secret_file, api_name, api_version, scopes, sep='-')
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]
    print(SCOPES)

    cred = None

    pickle_file = f'E:/python/token_{API_SERVICE_NAME}_{API_VERSION}.pickle'
    # print(pickle_file)

    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        with open(pickle_file, 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        print(API_SERVICE_NAME, 'service created successfully')
        return service
    except Exception as e:
        print('Unable to connect.')
        print(e)
        return None



def video_categories():
    video_categories = service.videoCategories().list(part='snippet', regionCode='US').execute()
    df = pd.DataFrame(video_categories.get('items'))
    return pd.concat([df['id'], df['snippet'].apply(pd.Series)[['title']]], axis=1)

BUCKET_NAME = 'shenkedokato'
VIDEO_FILE_NAME = 'Ys7mGc10IaQ.mp4'
API_NAME = 'youtube'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/youtube']
client_file = 'E:\python\client_secrets.json'
service = Create_Service(client_file, API_NAME, API_VERSION, SCOPES)
credentials = service_account.Credentials.from_service_account_file('C:/Users/Admin/Downloads/sa_key.json')
storage_client = storage.Client(credentials=credentials)
bucket = storage_client.bucket(BUCKET_NAME)
blob = bucket.blob(VIDEO_FILE_NAME)
print(video_categories())

"""
Step 1. Uplaod Video
"""
upload_time = (datetime.datetime.now() + datetime.timedelta(days=10)).isoformat() + '.000Z'
request_body = {
    'snippet': {
        'title': 'memecuon',
        'description': 'meme chu gi nua',
        'categoryId': 22,
        'tags': ['tags']
    },
    'status': {
        'privacyStatus': 'private',
        'publishedAt': upload_time,
        'selfDeclaredMadeForKids': False
    },
    'notifySubscribers': False
}

# video_file = options.file
media_file = MediaIoBaseUpload(io.BytesIO(blob.download_as_bytes()), mimetype='video/*', chunksize=1024*1024, resumable=True)
# media_file = MediaIoBaseUpload(blob.download_as_bytes(), mimetype='video/*', chunksize=1024*1024, resumable=True)

# print(media_file.size() / pow(1024, 2), 'mb')
# print(media_file.to_json())
# print(media_file.mimetype())

response_video_upload = service.videos().insert(
    part='snippet,status',
    body=request_body,
    media_body=media_file
).execute()
uploaded_video_id = response_video_upload.get('id')

