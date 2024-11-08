import asyncio
import os
import random

from bot.config import settings
from pyrogram import Client
from bot.utils import logger, load_from_json, save_list_to_file, save_to_json, get_all_lines

from better_proxy import Proxy

class Accounts:
    def __init__(self):
        self.LANG = settings.LANG
        self.api_id = settings.API_ID
        self.api_hash = settings.API_HASH

    @staticmethod
    def get_proxies() -> list[Proxy]:
        if settings.USE_PROXY_FROM_FILE:
            with open(file="bot/config/proxies.txt", encoding="utf-8-sig") as file:
                proxies = [Proxy.from_str(proxy=row.strip()).as_url for row in file]
        else:
            proxies = []
        return proxies

    @staticmethod
    def get_available_accounts(sessions: list):
        available_accounts = []

        if settings.USE_PROXY_FROM_FILE:
            proxies = get_proxies()
            proxies_cycle = cycle(proxies) if proxies else None
            for session in sessions:
                available_accounts.append({
                    'session_name': session,
                    'proxy': next(proxies_cycle) if proxies_cycle else None
                })
        else:
            accounts_from_json = load_from_json('sessions/accounts.json')
            if not accounts_from_json:
                if self.LANG == 'RU':
                    raise ValueError("Отсутствие учетных записей в файле sessions/accounts.json")
                elif self.LANG == 'UA':
                    raise ValueError("Немає облікових записів у session/accounts.json")
                else:
                    raise ValueError("Have not account's in sessions/accounts.json")
            for session in sessions:
                for saved_account in accounts_from_json:
                    if saved_account['session_name'] == session:
                        available_accounts.append(saved_account)
                        break

        return available_accounts

    def pars_sessions(self):
        sessions = [file.replace(".session", "") for file in os.listdir("sessions/") if file.endswith(".session")]

        if self.LANG == 'RU':
            logger.info(f"Поиск сессий: {len(sessions)}.")
        elif self.LANG == 'UA':
            logger.info(f"Пошук сесій: {len(sessions)}.")
        else:
            logger.info(f"Searched sessions: {len(sessions)}.")
            
        return sessions

    async def check_valid_account(self, account: dict):
        session_name = account['session_name']
        proxy = account['proxy']
        try:
            if proxy:
                proxy = Proxy.from_str(proxy)
                proxy_dict = dict(
                    scheme=proxy.protocol,
                    hostname=proxy.host,
                    port=proxy.port,
                    username=proxy.login,
                    password=proxy.password
                )
            else:
                proxy_dict = None

            client = Client(
                name=session_name,
                api_id=self.api_id,
                api_hash=self.api_hash,
                workdir='sessions/',
                proxy=proxy_dict
            )

            connect = await asyncio.wait_for(client.connect(), timeout=30)
            if connect:
                await client.get_me()
                await client.disconnect()
                return account
            else:
                await client.disconnect()
        except:
            pass

    async def check_valid_accounts(self, accounts: list):
        if self.LANG == 'RU':
            logger.info(f"Проверка аккаунтов на валидность...")
        elif self.LANG == 'UA':
            logger.info(f"Перевірка акаунтів на валідність...")
        else:
            logger.info(f"Checking accounts for valid...")

        tasks = []
        for account in accounts:
            tasks.append(asyncio.create_task(self.check_valid_account(account)))

        v_accounts = await asyncio.gather(*tasks)

        valid_accounts = [account for account, is_valid in zip(accounts, v_accounts) if is_valid]
        invalid_accounts = [account for account, is_valid in zip(accounts, v_accounts) if not is_valid]
        
        if self.LANG == 'RU':
            logger.success(f"Валидных аккаунтов: {len(valid_accounts)}; Невалидных: {len(invalid_accounts)}")
        elif self.LANG == 'UA':
            logger.success(f"Валідних акаунтів: {len(valid_accounts)}; Невалідних: {len(invalid_accounts)}")
        else:
            logger.success(f"Valid accounts: {len(valid_accounts)}; Invalid: {len(invalid_accounts)}")

        return valid_accounts, invalid_accounts

    async def get_accounts(self):
        sessions = self.pars_sessions()
        available_accounts = self.get_available_accounts(sessions)

        if not available_accounts:
            if self.LANG == 'RU':
                raise ValueError("У вас нет доступных аккаунтов!")
            elif self.LANG == 'UA':
                raise ValueError("У вас немає доступних акаунтів!")
            else:
                raise ValueError("Have not available accounts!")
        else:
            if self.LANG == 'RU':
                logger.success(f"Поиск доступных аккаунтов: {len(available_accounts)}.")
            elif self.LANG == 'UA':
                logger.success(f"Пошук доступних акаунтів: {len(available_accounts)}.")
            else:
                logger.success(f"Search available accounts: {len(available_accounts)}.")

        valid_accounts, invalid_accounts = await self.check_valid_accounts(available_accounts)

        if invalid_accounts:
            save_list_to_file(f"sessions/invalid_accounts.txt", invalid_accounts)
            if self.LANG == 'RU':
                logger.info(f"Сохранено {len(invalid_accounts)} недействительный(е) аккаунт(ы) в sessions/invalid_accounts.txt")
            elif self.LANG == 'UA':
                logger.info(f"Збережено {len(invalid_accounts)} недійсний(і) акаунт(и) у sessions/invalid_accounts.txt")
            else:
                logger.info(f"Saved {len(invalid_accounts)} invalid account(s) in sessions/invalid_accounts.txt")

        if not valid_accounts:
            if self.LANG == 'RU':
                raise ValueError("Нет валидных сессий")
            elif self.LANG == 'UA':
                raise ValueError("Немає валідних сесій")
            else:
                raise ValueError("Have not valid sessions")
        else:
            return valid_accounts

    async def create_sessions(self):
        while True:
            if self.LANG == 'RU':
                session_name = input('\nВведите название сессии (нажмите Enter, чтобы выйти).: ')
            elif self.LANG == 'UA':
                session_name = input('\nВведіть назву сесії (натисніть Enter, щоб вийти): ')
            else:
                session_name = input('\nInput the name of the session (press Enter to exit): ')
            
            if not session_name: return

            if settings.USE_PROXY_FROM_FILE:
                proxys = get_all_lines('bot/config/proxies.txt')
                proxy = random.choice(proxys) if proxys else None
            else:
                if self.LANG == 'RU':
                    proxy = (input("Введите прокси в формате scheme://username:password@ip:port (нажмите Enter, чтобы использовать без прокси).: ")).replace(' ', '')
                elif self.LANG == 'UA':
                    proxy = (input("Введіть проксі у форматі scheme://username:password@ip:port (натисніть Enter, щоб використовувати без проксі): ")).replace(' ', '')
                else:
                    proxy = input("Input the proxy in the format scheme://username:password@ip:port (press Enter to use without proxy): ")

            if proxy:
                proxy = Proxy.from_str(proxy)
                proxy_dict = dict(
                    scheme=proxy.protocol,
                    hostname=proxy.host,
                    port=proxy.port,
                    username=proxy.login,
                    password=proxy.password
                )
            else:
                proxy_dict = None
            
            if self.LANG == 'RU':
                phone_number = (input("Введите номер телефона учётной записи: ")).replace(' ', '')
            elif self.LANG == 'UA':
                phone_number = (input("Введіть номер телефону облікового запису: ")).replace(' ', '')
            else:
                phone_number = (input("Input the phone number of the account: ")).replace(' ', '')
            phone_number = '+' + phone_number if not phone_number.startswith('+') else phone_number

            client = Client(
                api_id=self.api_id,
                api_hash=self.api_hash,
                name=session_name,
                workdir='sessions/',
                phone_number=phone_number,
                proxy=proxy_dict,
                lang_code='ru'
            )

            async with client:
                me = await client.get_me()

            save_to_json(f'sessions/accounts.json', dict_={
                "session_name": session_name,
                "proxy": proxy
            })
            if self.LANG == 'RU':
                logger.success(f'Добавлен сессию @{me.username} ({user_data.first_name} {user_data.last_name}) | {me.phone_number}')
            elif self.LANG == 'UA':
                logger.success(f'Додано сесію @{me.username} ({user_data.first_name} {user_data.last_name}) | {me.phone_number}')
            else:
                logger.success(f'Added a session @{me.username} ({user_data.first_name} {user_data.last_name}) | {me.phone_number}')