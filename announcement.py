
import time
import telepot
from binance.client import Client
from binance.exceptions import BinanceAPIException

import ssl
ssl._create_default_https_context = ssl._create_unverified_context


class BinanceAnnouncementNotifier:
    def __init__(self, telegram_token, telegram_chat_id):
        self.client = Client()
        self.bot = telepot.Bot(telegram_token)
        self.telegram_chat_id = telegram_chat_id
        self.previous_announcement = self.get_latest_announcement()

        if self.previous_announcement:
            print("Running Binance Announcement Notifier...")
        else:
            print("Error: Couldn't fetch announcements. Exiting.")
            exit()

    def get_latest_announcement(self):
        try:
            announcement = self.client.get_system_status()
            return announcement
        except BinanceAPIException as e:
            print(e)
            return None

    def send_telegram_message(self, message):
        self.bot.sendMessage(chat_id=self.telegram_chat_id, text=message)

    def start_notifier(self):
        while True:
            time.sleep(5)
            latest_announcement = self.get_latest_announcement()

            if latest_announcement and latest_announcement != self.previous_announcement:
                self.send_telegram_message(f"🔔 New Binance Announcement 🔔\n\n{latest_announcement}")
                self.previous_announcement = latest_announcement
if __name__ == "__main__":
    telegram_token = '6020317499:AAEYk8iUT8htSnm0goTkLsvazZ449zXqVew'
    telegram_chat_id = '808400546'

    notifier = BinanceAnnouncementNotifier(telegram_token, telegram_chat_id)
    notifier.start_notifier()

