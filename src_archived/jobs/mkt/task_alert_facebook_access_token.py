from core.app_base import AppBase
from datetime import timedelta, datetime
import pandas as pd
import requests
from libs.storage_utils import load_sa_creds, load_gsheet_creds, insert_df_to_gsheet, load_df_from_gsheet
import pytz
import math

vn_tz = pytz.timezone('Asia/Saigon')

class TaskAlertFacebookAccessToken(AppBase):
    def __init__(self, config):
        super().__init__(config)
        self.dp_ops = self.get_param_config(['dp-ops'])
        self.sa_creds = load_sa_creds(self.dp_ops)
        self.gcreds = load_gsheet_creds(self.dp_ops)
        self.from_date = self.get_process_info(['from_date']).astimezone(vn_tz)
        self.to_date = self.get_process_info(['to_date']).astimezone(vn_tz)
        self.sheet_id = self.get_param_config(['gsheet_id'])
        self.worksheet_name = self.get_param_config(['worksheet_name'])
        self.fb_client_id = self.get_param_config(['mkt-auth','facebook_client_id'])
        self.fb_secret_id = self.get_param_config(['mkt-auth','facebook_secret_id'])
        self.alert_webhook_url = self.get_param_config(['alert_webhook_url'])

    def read_access_token(self):
        access_token = dict()
        df = load_df_from_gsheet(self.gcreds, self.sheet_id, self.worksheet_name)
        access_token = df.loc[0, 'access_token']
        expires_day = df.loc[0, 'expires_day']
        
        return {
            'access_token': access_token,
            'expires_day': expires_day
        }
    
    def is_access_token_expires(self):
        access_token = self.read_access_token()
        expires_day_str = access_token['expires_day']
        expires_day_date = datetime.strptime(expires_day_str, '%Y-%m-%d').date()
        current_date = datetime.now().date()
        if expires_day_date < current_date + timedelta(days=10):
            return True
        message_data = {
            "blocks": [
                {
                        "type": "divider"
                },
                {
                        "type": "section",
                        "text": {
                                "type": "mrkdwn",
                                "text": "access_token still working !!!!!! expires_day: {}".format(expires_day_date)
                        }
                }
            ]
        }
        r = requests.post( self.alert_webhook_url, json= message_data, headers={'Content-Type': 'application/json'})
        return False
        
    def alert(self):
        message_data = {
            "blocks": [
                    {
                            "type": "divider"
                    },
                    {
                            "type": "section",
                            "text": {
                                    "type": "mrkdwn",
                                    "text": "access_token expires!!!!!!"
                            }
                    }
            ]
        }   
        r = requests.post( self.alert_webhook_url, json= message_data, headers={'Content-Type': 'application/json'})
        
    def update_access_token(self):
        access_token = self.get_new_access_token()
        data = {
                'access_token': [access_token['access_token']],
                'expires_day': [access_token['expires_day']]
            }
        df = pd.DataFrame(data)
        insert_df_to_gsheet(self.gcreds, self.sheet_id, self.worksheet_name ,df)
        
    def get_new_access_token(self):
        params = {
            'grant_type': 'fb_exchange_token',
            'client_id': self.fb_client_id,
            'client_secret': self.fb_secret_id,
            'fb_exchange_token': self.read_access_token()['access_token'],
        }

        response = requests.get('https://graph.facebook.com/v14.0/oauth/access_token', params=params, verify= False)
        response = response.json()
        print(response)
        access_token = response['access_token']
        expires_in = response['expires_in']
        expires_day = math.floor(expires_in/ (24 *60 *60 ))
        current_date = datetime.now().date()
        expires_date = current_date + timedelta(days= expires_day)
        expires_date = expires_date.strftime("%Y-%m-%d")
        return {
            'access_token': access_token,
            'expires_day': expires_date
        }

    def execute(self):
        if self.is_access_token_expires():
            self.alert()
            self.update_access_token()
        
        
        
