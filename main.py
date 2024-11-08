import asyncio
from contextlib import suppress
import os
import argparse

from bot.config import settings
from bot.utils.telegram import Accounts
from bot.core.tapper import run_tapper

import inquirer

start_text = r"""
 ███████████  █████       █████  █████ ██████   ██████
░░███░░░░░███░░███       ░░███  ░░███ ░░██████ ██████ 
 ░███    ░███ ░███        ░███   ░███  ░███░█████░███ 
 ░██████████  ░███        ░███   ░███  ░███░░███ ░███ 
 ░███░░░░░███ ░███        ░███   ░███  ░███ ░░░  ░███ 
 ░███    ░███ ░███      █ ░███   ░███  ░███      ░███ 
 ███████████  ███████████ ░░████████   █████     █████
░░░░░░░░░░░  ░░░░░░░░░░░   ░░░░░░░░   ░░░░░     ░░░░░ 
Soft's author: https://t.me/CatSnowdrop
My Telegram channel: https://t.me/CryptoCats_tg

"""

async def main():
    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        parser = argparse.ArgumentParser()
        parser.add_argument('-a', '--action', type=int, help='Action to perform')
        action = parser.parse_args().action

        
        if not action:
            print(start_text)
            if settings.LANG == 'RU':
                questions = [
                    inquirer.List(
                        "action",
                        message="Какое действие вы хотите выполнить?",
                        choices=["Запустить софт", "Создать сессию"],
                    ),
                ]
                action_mapping = {
                    'Запустить софт': 1,
                    'Создать сессию': 2
                }
            elif settings.LANG == 'UA':
                questions = [
                    inquirer.List(
                        "action",
                        message="Яку дію ви хочете виконати?",
                        choices=["Запустити софт", "Створити сесію"],
                    ),
                ]
                action_mapping = {
                    'Запустити софт': 1,
                    'Створити сесію': 2
                }
            else:
                questions = [
                    inquirer.List(
                        "action",
                        message="What do you want to do?",
                        choices=["Start soft", "Create sessions"],
                    ),
                ]
                action_mapping = {
                    'Start soft': 1,
                    'Create sessions': 2
                }
            answers = inquirer.prompt(questions)

            action = action_mapping[answers['action']]

        if not os.path.exists('sessions'): os.mkdir('sessions')

        if settings.USE_PROXY_FROM_FILE:
            if not os.path.exists('bot/config/proxies.txt'):
                with open('bot/config/proxies.txt', 'w') as f:
                    f.write("")
        else:
            if not os.path.exists('sessions/accounts.json'):
                with open("sessions/accounts.json", 'w') as f:
                    f.write("[]")

        if action == 2:
            await Accounts().create_sessions()

        if action == 1:
            accounts = await Accounts().get_accounts()
            tasks = []
            for thread, account in enumerate(accounts):
                session_name = account['session_name']
                proxy = account['proxy']
                tasks.append(
                    asyncio.create_task(
                        run_tapper(
                            session_name=session_name,
                            thread=thread,
                            proxy=proxy
                        )
                    )
                )
            await asyncio.gather(*tasks)


if __name__ == '__main__':
    with suppress(KeyboardInterrupt):
        asyncio.run(main())