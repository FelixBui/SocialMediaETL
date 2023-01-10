import telegram
from telegram.utils.request import Request


class TelegramUtils(object):
    def __init__(self, config: dict):
        tkn = config["token"]
        https_proxy = config.get("https_proxy")
        if https_proxy:
            pp = Request(proxy_url=https_proxy)
            self.bot = telegram.Bot(token=tkn, request=pp)
        else:
            self.bot = telegram.Bot(token=tkn)

    def send_message(self, user_id, msg, parse_mode="Markdown"):
        self.bot.send_message(user_id, msg, parse_mode=parse_mode)

    def send_message_with_attachment(self, user_id, msg, file):
        self.bot.send_message(user_id, msg)
        self.bot.send_document(user_id, msg, file)

    def send_photo(self, user_id, msg, file):
        self.bot.send_photo(user_id, caption=msg, photo=file)
