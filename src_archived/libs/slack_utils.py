import requests
import json
from datetime import datetime, timedelta

class SlackBot:
    def __init__(self, bot_url ):
        self.bot_url = bot_url

    def insert_msg(self, msg, msg_block):
        template = {
            "blocks":[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": msg
                    }
                }
            ]
        }
        if msg_block != "":
            template['blocks'].append(msg_block)
        return template

    def send_msg(self, msg, msg_block):
        data = self.insert_msg(msg,msg_block)
        res = requests.post( self.bot_url,
                      json= data,
                      headers={'Content-Type': 'application/json'})

# Example 
# my_bot = slack.SlackBot(self.slack_config['bot_url'])
# my_bot.send_msg( self.slack_config['message'], self.slack_config['message_block'])