import asyncio
import os

from airflow.notifications.basenotifier import BaseNotifier
from telegram import Bot


class TelegramNotifier(BaseNotifier):
    @staticmethod
    async def send_tg_notification(notifier_message):
        telegram_bot_token = os.environ['TELEGRAM_NOTIFIER_BOT_TOKEN']
        telegram_chat_id = os.environ['TELEGRAM_NOTIFIER_CHAT_ID']

        bot = Bot(token=telegram_bot_token)
        await bot.sendMessage(
            chat_id=telegram_chat_id, text=notifier_message, parse_mode='MarkdownV2'
        )

    def notify(self, context):
        ti = context['ti']

        message_template = (
            f'💀 Task failed 💀\n\n'
            f'DAG: `{ti.dag_id}`\n'
            f'Task id: `{ti.task_id}`\n\n'
            f'Logs here: `{ti.log_url}`'
        )

        asyncio.run(self.send_tg_notification(notifier_message=message_template))
