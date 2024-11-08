import asyncio
import random
from random import randint, uniform
import string
import time
from json import loads
from urllib.parse import unquote, quote

from bot.config import settings
from bot.utils import logger, load_from_json, add_list_to_json
from bot.exceptions import InvalidSession, NeedReLoginError, NeedRefreshTokenError
from .agents import generate_random_user_agent
from .headers import headers

import aiohttp
from aiocfscrape import CloudflareScraper
from aiohttp_socks import ProxyConnector
from better_proxy import Proxy

from pyrogram import Client
from pyrogram.raw.functions.messages import RequestAppWebView
from pyrogram.raw.types import InputBotAppShortName
from pyrogram.errors import (Unauthorized, UserDeactivated, AuthKeyUnregistered, FloodWait, UserDeactivatedBan,
                             AuthKeyDuplicated, SessionExpired, SessionRevoked)


from faker import Faker

async def run_tapper(thread: int, session_name: str, proxy: [str, None]):
    try:
        await Tapper(thread=thread, session_name=session_name, proxy=proxy).run()
    except InvalidSession:
        logger.error(f"{tg_client.name} | Invalid Session")

class Tapper:
    def __init__(self, thread: int, session_name: str, proxy: [str, None]):
        self.gateway_url = "https://gateway.blum.codes"
        self.wallet_url = "https://wallet-domain.blum.codes"
        self.subscription_url = "https://subscription.blum.codes"

        self.game_url = "https://game-domain.blum.codes"
        self.earn_domain = "https://earn-domain.blum.codes"
        self.user_url = "https://user-domain.blum.codes"
        self.tribe_url = "https://tribe-domain.blum.codes"
        
        self.task_database = "https://raw.githubusercontent.com/zuydd/database/main/blum.json"
        self.task_payload_server = "http://cryptocats-blum.duckdns.org:9876"
    
        self.LANG = settings.LANG
        self.account = session_name + '.session'
        self.thread = thread
        if settings.USE_REF:
            self.ref_token = 'ref_jYYgDsziXX' if random.random() <= 0.3 else settings.REF_LINK.split('=')[1]
        else:
            self.ref_token = ''
        self.proxy = proxy
        connector = ProxyConnector().from_url(proxy) if proxy else None

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

        self.tg_client = Client(
            name=session_name,
            api_id=settings.API_ID,
            api_hash=settings.API_HASH,
            workdir='sessions/',
            proxy=proxy_dict,
            lang_code='ru'
        )

        headers['User-Agent'] = self.get_user_agent(session_name)
        headers["Origin"] = "https://telegram.blum.codes"
        self.headers = headers

        self.session = CloudflareScraper(headers=headers, connector=connector)


    @staticmethod
    def error_wrapper(method):
        async def wrapper(self, *arg, **kwargs):
            try:
                return await method(self, *arg, **kwargs)
            except NeedRefreshTokenError:
                await self.refresh_tokens()
                return await method(self, *arg, **kwargs)
            except NeedReLoginError:
                raise NeedReLoginError
            except Exception as e:
                logger.error(f"Error on Tapper.{method.__name__} | {type(e).__name__}: {e}")
        return wrapper


    async def refresh_tokens(self):
        if "Authorization" in self.session.headers:
            del self.session.headers["Authorization"]
        resp = await self.session.post(f"{self.user_url}/api/v1/auth/refresh", json={'refresh': self.refresh_token})
        if resp.status == 401:
            raise NeedReLoginError()
        logger.debug("Tokens have been successfully updated.")
        resp_json = await resp.json()
        self.refresh_token = resp_json.get('refresh', '')
        self.session.headers["Authorization"] = "Bearer " + resp_json.get('access', '')


    def get_user_agent(self, session_name):
        file = 'sessions/accounts.json'
        accounts_from_json = load_from_json(file)
        user_agent_str = None
        for saved_account in accounts_from_json:
            if saved_account['session_name'] == session_name:
                if 'user_agent' in saved_account:
                    user_agent_str = saved_account['user_agent']
        if user_agent_str != None and user_agent_str != "":
            return user_agent_str
        else:
            user_agent_str = generate_random_user_agent(device_type='android', browser_type='chrome')
            add_list_to_json(file, 'session_name', session_name, 'user_agent', user_agent_str)
            return user_agent_str


    async def check_proxy(self):
        try:
            resp = await self.session.get('https://api.ipify.org?format=json', timeout=aiohttp.ClientTimeout(5))
            ip = (await resp.json()).get('ip')
            logger.info(f"Thread {self.thread} | {self.account} | Proxy IP: {ip}")
        except Exception as error:
            logger.error(f"Thread {self.thread} | Proxy: {self.proxy} | Error: {error}")

    async def logout(self):
        await self.session.close()

    async def check_payload_server(self, full_test: bool = False) -> bool:
        try:
            async with self.session.get(f"{self.task_payload_server}/status", timeout=3) as response:
                if response.status == 200 and (await response.json()).get("status") == "ok" and not full_test:
                    return True
                if full_test:
                    test_game_id = "ad7cd4cd-29d1-4548-89a3-91301996ef31"
                    payload = await get_payload(self.task_payload_server, test_game_id, 150)
                    if len(payload) == 684:
                        return True
                return False
        except (TimeoutError, ClientConnectorError):
            pass
        return False


    async def get_payload(self, game_id: str, blum_points: int | str) -> str | None:
        data = {
            "gameId": game_id,
            "earnedAssets": {
                "CLOVER": {
                    "amount": str(blum_points)
                }
            }
        }
        async with self.session.post(url=f"{self.task_payload_server}/getPayload", json=data) as response:
            data = await response.json()
            if response.status == 200 and data.get("payload"):
                return data.get("payload")
            raise Exception(f"Payload Server Error: {data.get('error')}")


    @error_wrapper
    async def claim_game(self, payload: str) -> bool:
        resp = await self.session.post(f"{self.game_url}/api/v2/game/claim", json={"payload": payload})
        txt = await resp.text()
        if resp.status != 200:
            logger.error(f"Thread {self.thread} | {self.account} | error claim_game. response status {resp.status}: {txt}")
        return True if txt == 'OK' else False

    @error_wrapper
    async def start_game(self):
        resp = await self.session.post(f"{self.game_url}/api/v2/game/play")
        response_data = await resp.json()
        return response_data.get("gameId")


    async def play_drop_game(self):
        if settings.PLAY_GAMES is not True or not self.play_passes:
            return

        if not await self.check_payload_server(full_test=False):  # full_test=True
            if self.LANG == 'RU':
                logger.warning(f"Поток {self.thread} | {self.account} | Payload сервер недоступен, возможно, тех. работы.")
            elif self.LANG == 'UA':
                logger.warning(f"Поток {self.thread} | {self.account} | Payload сервер недоступний, можливо, тех. роботи.")
            else:
                logger.warning(f"Thread {self.thread} | {self.account} | Payload server not available, maybe technical work.")
            return

        tries = 3

        while self.play_passes:
            try:
                await asyncio.sleep(uniform(1, 3))
                game_id = await self.start_game()

                if not game_id or not await self.check_payload_server(full_test=False):
                    reason = "error get game_id" if not game_id else "payload server not available"
                    if self.LANG == 'RU':
                        logger.info(f"Поток {self.thread} | {self.account} | Не удалось начать игру! Причина: {reason}! Повторяем попытку!")
                    elif self.LANG == 'UA':
                        logger.info(f"Поток {self.thread} | {self.account} | Не вдалося розпочати гру! Причина: {reason}! Повторюємо спробу!")
                    else:
                        logger.info(f"Thread {self.thread} | {self.account} | Couldn't start play in game! Reason: {reason}! Trying again!")
                    tries -= 1
                    if tries <= 0:
                        if self.LANG == 'RU':
                            return logger.warning(f"Поток {self.thread} | {self.account} | Попытки закончились, пропускаем игры")
                        elif self.LANG == 'UA':
                            return logger.warning(f"Поток {self.thread} | {self.account} | Закінчились спроби, пропускаємо ігри")
                        else:
                            return logger.warning(f"Thread {self.thread} | {self.account} | No more trying, gonna skip games")
                    continue

                sleep_time = uniform(30, 40)
                if self.LANG == 'RU':
                    logger.info(f"Поток {self.thread} | {self.account} | Начало игры id {game_id}. <r>Сон {int(sleep_time)} сек...</r>")
                elif self.LANG == 'UA':
                    logger.info(f"Поток {self.thread} | {self.account} | Початок гри id {game_id}. <r>Сон {int(sleep_time)} сек...</r>")
                else:
                    logger.info(f"Thread {self.thread} | {self.account} | Started playing game id {game_id}. <r>Sleep {int(sleep_time)}s...</r>")
                await asyncio.sleep(sleep_time)
                blum_points = randint(settings.POINTS[0], settings.POINTS[1])
                payload = await self.get_payload(game_id, blum_points)
                status = await self.claim_game(payload)
                await asyncio.sleep(uniform(1, 2))
                if status:
                    if self.LANG == 'RU':
                        logger.success(f"Поток {self.thread} | {self.account} | Игра завершена! Награда: <g>{blum_points}</g> BP. Осталось билетов: {self.play_passes}")
                    elif self.LANG == 'UA':
                        logger.success(f"Поток {self.thread} | {self.account} | Гру завершено! Нагорода: <g>{blum_points}</g> BP. Залишилось квитків: {self.play_passes}")
                    else:
                        logger.success(f"Thread {self.thread} | {self.account} | Finish play in game! Reward: <g>{blum_points}</g> BP. {self.play_passes} passes left")
                await self.update_balance()
                sleep_time = uniform(1, 5)
            except Exception as e:
                if self.LANG == 'RU':
                    logger.error(f"Поток {self.thread} | {self.account} | Возникла ошибка во время игры: {type(e)} - {e}")
                elif self.LANG == 'UA':
                    logger.error(f"Поток {self.thread} | {self.account} | Виникла помилка під час гри: {type(e)} - {e}")
                else:
                    logger.error(f"Thread {self.thread} | {self.account} | Error occurred during play game: {type(e)} - {e}")


    @error_wrapper
    async def validate_task(self, task_id, keyword: str) -> bool:
        payload = {'keyword': keyword}
        resp = await self.session.post(f'{self.earn_domain}/api/v1/tasks/{task_id}/validate', json=payload)
        resp_json = await resp.json()
        if resp_json.get('status') == "READY_FOR_CLAIM":
            return True
        if resp_json.get('message') == "Incorrect task keyword":
            return False
        logger.error(f"Thread {self.thread} | {self.account} | validate_task error: {resp_json}")


    @error_wrapper
    async def claim_task(self, task_id):
        resp = await self.session.post(f'{self.earn_domain}/api/v1/tasks/{task_id}/claim')
        resp_json = await resp.json()
        if resp_json.get('status') == "FINISHED":
            return True
        logger.error(f"Thread {self.thread} | {self.account} | claim_task error: {resp_json}")


    @error_wrapper
    async def start_task(self, task_id):
        resp = await self.session.post(f'{self.earn_domain}/api/v1/tasks/{task_id}/start')
        resp_json = await resp.json()
        if resp_json.get("status") == 'STARTED':
            return True
        raise Exception(f"unknown response structure. status: {resp.status}. body: {resp_json}")


    async def get_tasks(self):
        try:
            resp = await self.session.get(f'{self.earn_domain}/api/v1/tasks')
            if resp.status not in [200, 201]:
                return None
            resp_json = await resp.json()

            collected_tasks = []
            for section in resp_json:
                collected_tasks.extend(section.get('tasks', []))
                for sub_section in section.get("subSections"):
                    collected_tasks.extend(sub_section.get('tasks', []))

            for task in collected_tasks:
                if task.get("subTasks"):
                    collected_tasks.extend(task.get("subTasks"))

            unique_tasks = {}

            task_types = ("SOCIAL_SUBSCRIPTION", "INTERNAL", "SOCIAL_MEDIA_CHECK")
            for task in collected_tasks:
                if  task['status'] == "NOT_STARTED" and task['type'] in task_types or \
                    task['status'] == "READY_FOR_CLAIM" or \
                    task['status'] == "READY_FOR_VERIFY" and task['validationType'] == 'KEYWORD':
                    unique_tasks.update({task.get("id"): task})
            logger.debug(f"Thread {self.thread} | {self.account} | Loaded {len(unique_tasks.keys())} tasks")
            return unique_tasks.values()
        except Exception as error:
            if self.LANG == 'RU':
                logger.error(f"Поток {self.thread} | {self.account} | Ошибка при получении заданий: {error}")
            elif self.LANG == 'UA':
                logger.error(f"Поток {self.thread} | {self.account} | Помилка при отриманні завдань: {error}")
            else:
                logger.error(f"Thread {self.thread} | {self.account} | Get tasks error: {error}")
            return []


    async def get_blum_database(self) -> dict | None:
        async with CloudflareScraper() as session:
            request = await session.get(url=self.task_database, headers={"Accept": "application/json"})
            if request.status == 200:
                body = await request.text()
                return loads(body)


    async def check_tasks(self):
        if settings.AUTO_TASKS is not True:
            return
        await asyncio.sleep(uniform(1, 3))
        blum_database = await self.get_blum_database()
        tasks_codes = blum_database.get('tasks')
        tasks = await self.get_tasks()

        for task in tasks:
            await asyncio.sleep(uniform(0.5, 1))

            if task["id"] in ("", "8b2324a1-931c-4061-81d7-f759f1653001"):###### Задания ловушки
                continue
            if not task.get('status'):
                continue
            if task.get('status') == "NOT_STARTED":
                if self.LANG == 'RU':
                    logger.info(f"Поток {self.thread} | {self.account} | Приступаем к выполнению задания «{task['title']}»")
                elif self.LANG == 'UA':
                    logger.info(f"Поток {self.thread} | {self.account} | Розпочинаємо виконання завдання «{task['title']}»")
                else:
                    logger.info(f"Thread {self.thread} | {self.account} | Started doing task - «{task['title']}»")
                await self.start_task(task_id=task["id"])
            elif task['status'] == "READY_FOR_CLAIM":
                status = await self.claim_task(task_id=task["id"])
                if status:
                    if self.LANG == 'RU':
                        logger.success(f"Поток {self.thread} | {self.account} | Награда за задание «{task['title']}» получена")
                    elif self.LANG == 'UA':
                        logger.success(f"Поток {self.thread} | {self.account} | Нагорода за завдання «{task['title']}» отримана")
                    else:
                        logger.success(f"Thread {self.thread} | {self.account} | Claimed task - «{task['title']}»")
            elif task['status'] == "READY_FOR_VERIFY" and task['validationType'] == 'KEYWORD':
                await asyncio.sleep(uniform(1, 3))
                keyword = [item["answer"] for item in tasks_codes if item['id'] == task["id"]]
                if not keyword:
                    continue
                status = await self.validate_task(task["id"], keyword.pop())
                if not status:
                    continue
                if self.LANG == 'RU':
                    logger.success(f"Поток {self.thread} | {self.account} | Задание «{task['title']}» отправлено на проверку")
                elif self.LANG == 'UA':
                    logger.success(f"Поток {self.thread} | {self.account} | Завдання «{task['title']}» відправлено на перевірку")
                else:
                    logger.success(f"Thread {self.thread} | {self.account} | Validated task - «{task['title']}»")
                status = await self.claim_task(task["id"])
                if status:
                    if self.LANG == 'RU':
                        logger.success(f"Поток {self.thread} | {self.account} | Награда за задание «{task['title']}» получена")
                    elif self.LANG == 'UA':
                        logger.success(f"Поток {self.thread} | {self.account} | Нагорода за завдання «{task['title']}» отримана")
                    else:
                        logger.success(f"Thread {self.thread} | {self.account} | Claimed task - «{task['title']}»")
        await asyncio.sleep(uniform(0.5, 1))
        await self.update_balance()


    async def check_tribe(self):
        try:
            resp = await self.session.get(f'{self.tribe_url}/api/v1/tribe/my')
            resp_json = await resp.json()
            if resp.status == 404 and resp_json.get("data"):
                my_tribe = resp_json.get("data")
            if resp.status == 424:
                resp_json.update({"blum_bug": True})
                my_tribe = resp_json
            if resp.status == 200 and resp_json.get("chatname"):
                my_tribe = resp_json
            else:
                raise Exception(f"Unknown structure. status {resp.status}, body: {resp_json}")
            if my_tribe.get("blum_bug"):
                if self.LANG == 'RU':
                    return logger.warning(f"Поток {self.thread} | {self.account} | <r>Blum or TG Bug!</r> Учетная запись в трайбле, но трайбл не загружается и не отсоединяется.")
                elif self.LANG == 'UA':
                    return logger.warning(f"Поток {self.thread} | {self.account} | <r>Blum or TG Bug!</r> Обліковий запис у трайблі, але трайбл не завантажується і не від'єднується.")
                else:
                    return logger.warning(f"Thread {self.thread} | {self.account} | <r>Blum or TG Bug!</r> Account in tribe, but tribe not loading and leaving.")
            if my_tribe.get("title"):
                if self.LANG == 'RU':
                    logger.info(f"Поток {self.thread} | {self.account} | Мой трайбл <g>{my_tribe.get('title')}</g> ({my_tribe.get('chatname')})")
                elif self.LANG == 'UA':
                    logger.info(f"Поток {self.thread} | {self.account} | Мій трайбл <g>{my_tribe.get('title')}</g> ({my_tribe.get('chatname')})")
                else:
                    logger.info(f"Thread {self.thread} | {self.account} | My tribe <g>{my_tribe.get('title')}</g> ({my_tribe.get('chatname')})")

            chat_name = settings.TRIBE_CHAT_TAG
            if not chat_name or my_tribe.get("chatname") == chat_name:
                return
            await asyncio.sleep(uniform(0.1, 0.5))

            resp = await self.session.get(f'{self.tribe_url}/api/v1/tribe?search={chat_name}')
            resp_json = await resp.json()
            if resp.status != 200:
                raise Exception(f"Failed search_tribe: {resp_json}")
            result = resp_json.get("items")
            if result:
                chat_tribe = result.pop(0)

            if not chat_tribe.get("id"):
                if self.LANG == 'RU':
                    logger.warning(f"Поток {self.thread} | {self.account} | Тег чата трайбла из конфигурации '{chat_name}' не найден")
                elif self.LANG == 'UA':
                    logger.warning(f"Поток {self.thread} | {self.account} | Тег чату трайбла з конфігурації  '{chat_name}' не знайдено")
                else:
                    logger.warning(f"Thread {self.thread} | {self.account} | Tribe chat tag from config '{chat_name}' not found")
                settings.TRIBE_CHAT_TAG = None
                return

            if my_tribe.get('id') != chat_tribe.get('id'):
                await asyncio.sleep(uniform(0.1, 0.5))
                if my_tribe.get("title"):

                    resp = await self.session.post(f'{self.tribe_url}/api/v1/tribe/leave', json={})
                    text = await resp.text()
                    if text == 'OK':
                        if self.LANG == 'RU':
                            logger.info(f"Поток {self.thread} | {self.account} | <r>Покидем трайбл {my_tribe.get('title')}</r>")
                        elif self.LANG == 'UA':
                            logger.info(f"Поток {self.thread} | {self.account} | <r>Покидаемо трайбл {my_tribe.get('title')}</r>")
                        else:
                            logger.info(f"Thread {self.thread} | {self.account} | <r>Leave tribe {my_tribe.get('title')}</r>")
                    else:
                        raise Exception(f"Failed leave_tribe: {text}")

                resp = await self.session.post(f'{self.tribe_url}/api/v1/tribe/{tribe_id}/join', json={})
                text = await resp.text()
                if text == 'OK':
                    if self.LANG == 'RU':
                        logger.success(f"Поток {self.thread} | {self.account} | Успешное присоединение к трайблу {chat_tribe['title']}")
                    elif self.LANG == 'UA':
                        logger.success(f"Поток {self.thread} | {self.account} | Успішне приєднання до трайблу {chat_tribe['title']}")
                    else:
                        logger.success(f"Thread {self.thread} | {self.account} | Joined to tribe {chat_tribe['title']}")
                else:
                    raise Exception(f"Failed join_tribe: {text}")

        except Exception as error:
            if self.LANG == 'RU':
                logger.error(f"Поток {self.thread} | {self.account} | Присоединение к трайблу: {error}")
            elif self.LANG == 'UA':
                logger.error(f"Поток {self.thread} | {self.account} | Приєднання до трайблу: {error}")
            else:
                logger.error(f"Thread {self.thread} | {self.account} | Join tribe: {error}")


    @error_wrapper
    async def elig_dogs(self):
        resp = await self.session.get(f'{self.game_url}/api/v2/game/eligibility/dogs_drop')
        data = await resp.json()
        if resp.status == 200:
            return data.get('eligible', False)
        raise Exception(f"Unknown eligibility status: {data}")


    @error_wrapper
    async def check_friends_balance(self):
        resp = await self.session.get(f"{self.user_url}/api/v1/friends/balance")
        resp_json = await resp.json()
        if resp.status != 200:
            raise Exception(f"error from get friends balance: {resp_json}")
        balance = resp_json
        if not balance or not balance.get("canClaim", False) or not balance.get("amountForClaim", 0):
            logger.debug(f"Thread {self.thread} | {self.account} | Not available friends balance.")
            return
        await asyncio.sleep(uniform(1, 3))
        
        resp = await self.session.post(f"{self.user_url}/api/v1/friends/claim")
        resp_json = await resp.json()
        if resp.status != 200:
            raise Exception(f"Failed claim_friends_balance: {resp_json}")
        amount = resp_json.get("claimBalance")
        if self.LANG == 'RU':
            logger.success(f"Поток {self.thread} | {self.account} | Получено <g>{amount}</g> BP от друзей")
        elif self.LANG == 'UA':
            logger.success(f"Поток {self.thread} | {self.account} | Отримано <g>{amount}</g> BP від друзів")
        else:
            logger.success(f"Thread {self.thread} | {self.account} | Claim <g>{amount}</g> BP from friends balance!")


    @error_wrapper
    async def check_farming(self):
        await asyncio.sleep(uniform(1, 3))
        if self.farming_data and self.farming_data.get("farming_delta_times") >= 0:
            if self.LANG == 'RU':
                logger.info(f"Поток {self.thread} | {self.account} | Идет процесс фарминга... Нафармлено: {self.farming_data.get('balance')} BP")
            elif self.LANG == 'UA':
                logger.info(f"Поток {self.thread} | {self.account} | Йде процес фармінгу... Нафармлено: {self.farming_data.get('balance')} BP")
            else:
                logger.info(f"Thread {self.thread} | {self.account} | Farming process... Farmed balance: {self.farming_data.get('balance')} BP")
            return
        elif self.farming_data:
            resp = await self.session.post(f"{self.game_url}/api/v1/farming/claim")
            resp_json = await resp.json()
            for key in ['availableBalance', 'playPasses', 'isFastFarmingEnabled', 'timestamp']:
                if key not in resp_json:
                    raise Exception(f"Unknown structure claim_farm result: {resp_json}")
            if self.LANG == 'RU':
                logger.success(f"Поток {self.thread} | {self.account} | Получено c фарминга <g>{self.farming_data.get('balance')}</g> BP")
            elif self.LANG == 'UA':
                logger.success(f"Поток {self.thread} | {self.account} | Отримано з фармінгу <g>{self.farming_data.get('balance')}</g> BP")
            else:
                logger.success(f"Thread {self.thread} | {self.account} | Claim farm <g>{self.farming_data.get('balance')}</g> BP")
            await asyncio.sleep(uniform(0.1, 0.5))
        resp = await self.session.post(f"{self.game_url}/api/v1/farming/start")
        data = await resp.json()
        if resp.status != 200:
            if self.LANG == 'RU':
                logger.error(f"Поток {self.thread} | {self.account} | Ошибка начала фарминга")
            elif self.LANG == 'UA':
                logger.error(f"Поток {self.thread} | {self.account} | Помилка початку фармінгу")
            else:
                logger.error(f"Thread {self.thread} | {self.account} | Failed start farming")
        if self.LANG == 'RU':
            logger.info(f"Поток {self.thread} | {self.account} | Начало фарминга!")
        elif self.LANG == 'UA':
            logger.info(f"Поток {self.thread} | {self.account} | Початок фармінгу!")
        else:
            logger.info(f"Thread {self.thread} | {self.account} | Start farming!")
        await asyncio.sleep(uniform(0.1, 0.5))
        await self.update_balance()


    @error_wrapper
    async def update_balance(self):
        resp = await self.session.get(f"{self.game_url}/api/v1/user/balance")
        data = await resp.json()
        is_normal = True
        for key in ("availableBalance", "playPasses", "isFastFarmingEnabled", "timestamp", "farming", "isFastFarmingEnabled"):
            if key not in data and key not in ("farming", "isFastFarmingEnabled"):
                is_normal = False
        if is_normal:
            balance = data
        else:
            if self.LANG == 'RU':
                logger.error(f"Поток {self.thread} | {self.account} | Неизвестная структура баланса. status: {resp.status}, body: {data}")
            elif self.LANG == 'UA':
                logger.error(f"Поток {self.thread} | {self.account} | Невідома структура балансу. status: {resp.status}, body: {data}")
            else:
                logger.error(f"Thread {self.thread} | {self.account} | Unknown balance structure. status: {resp.status}, body: {data}")
        if not balance:
            raise Exception("Failed to get balance.")
        self.farming_data = balance.get("farming")
        if self.farming_data:
            self.farming_data.update({"farming_delta_times": self.farming_data.get("endTime") - balance.get("timestamp")})
        self.play_passes = balance.get("playPasses", 0)
        if self.LANG == 'RU':
            logger.info(f"Поток {self.thread} | {self.account} | Баланс: <g>{balance.get('availableBalance')}</g> BP. Билетов: <g>{self.play_passes}</g>.")
        elif self.LANG == 'UA':
            logger.info(f"Поток {self.thread} | {self.account} | Баланс: <g>{balance.get('availableBalance')}</g> BP. Квитків: <g>{self.play_passes}</g>.")
        else:
            logger.info(f"Thread {self.thread} | {self.account} | Balance: <g>{balance.get('availableBalance')}</g> BP. Play passes: <g>{self.play_passes}</g>.")


    @error_wrapper
    async def check_daily_reward(self) -> str | None:
        resp = await self.session.get(f"{self.game_url}/api/v1/daily-reward?offset=-180")
        data = await resp.json()
        if data.get("message") == "Not Found":
            daily_reward = None
        days = data.get("days")
        
        if days:
            current_reward: dict = days[-1].get("reward")
            daily_reward = f"passes: {current_reward.get('passes')}, BP: {current_reward.get('points')}"
        
        if daily_reward:
            if self.LANG == 'RU':
                logger.info(f"Поток {self.thread} | {self.account} | Доступное ежедневное вознаграждение: {daily_reward}")
            elif self.LANG == 'UA':
                logger.info(f"Поток {self.thread} | {self.account} | Доступна щоденна винагорода: {daily_reward}")
            else:
                logger.info(f"Thread {self.thread} | {self.account} | Available {daily_reward} daily reward.")
            resp = await self.session.post(f"{self.game_url}/api/v1/daily-reward?offset=-180")
            txt = await resp.text()
            if resp.status == 200 and txt == "OK":
                if self.LANG == 'RU':
                    logger.success(f"Поток {self.thread} | {self.account} | Ежедневная награда получена!")
                elif self.LANG == 'UA':
                    logger.success(f"Поток {self.thread} | {self.account} | Щоденну винагороду отримано!")
                else:
                    logger.success(f"Thread {self.thread} | {self.account} | Daily reward claimed!")
        else:
            if self.LANG == 'RU':
                logger.info(f"Поток {self.thread} | {self.account} | Ежедневное вознаграждение недоступно.")
            elif self.LANG == 'UA':
                logger.info(f"Поток {self.thread} | {self.account} | Щоденна винагорода недоступна.")
            else:
                logger.info(f"Thread {self.thread} | {self.account} | No daily reward available.")


    async def login(self):
        if self.proxy:
            await self.check_proxy()
        self.session.headers.pop('Authorization', None)
        query = await self.get_tg_web_data()
        web_data = {"query": query}

        if query is None:
            if self.LANG == 'RU':
                logger.error(f"Поток {self.thread} | {self.account} | Сессия {self.account} недействительна")
            elif self.LANG == 'UA':
                logger.error(f"Поток {self.thread} | {self.account} | Сесія {self.account} недійсна")
            else:
                logger.error(f"Thread {self.thread} | {self.account} | Session {self.account} invalid")
            await self.logout()
            return None

        while True:
            if settings.USE_REF and not web_data.get("username"):
                web_data.update({"username": web_data.get("username"), "referralToken": self.ref_token})
            resp = await self.session.post(f"{self.user_url}/api/v1/auth/provider/PROVIDER_TELEGRAM_MINI_APP", json=web_data)
            if resp.status == 520 or resp.status == 400:
                if self.LANG == 'RU':
                    logger.warning(f"Поток {self.thread} | {self.account} | Повторная попытка входа...")
                elif self.LANG == 'UA':
                    logger.warning(f"Поток {self.thread} | {self.account} | Повторна спроба входу...")
                else:
                    logger.warning(f"Thread {self.thread} | {self.account} | Relogin...")
                await asyncio.sleep(10)
                continue
            else:
                break

        resp_json = await resp.json()
        token = resp_json.get("token", {})
        self.refresh_token = token.get('refresh', '')
        self.session.headers["Authorization"] = "Bearer " + token.get('access', '')


    async def get_tg_web_data(self):
        try:
            with_tg = True
            if not self.tg_client.is_connected:
                with_tg = False
                await self.tg_client.connect()

            if not (await self.tg_client.get_me()).username:
                while True:
                    username = Faker('en_US').name().replace(" ", "") + '_' + ''.join(random.choices(string.digits, k=random.randint(3, 6)))
                    if await self.tg_client.set_username(username):
                        if self.LANG == 'RU':
                            logger.success(f"Поток {self.thread} | {self.account} | Установка имени пользователя @{username}")
                        elif self.LANG == 'UA':
                            logger.success(f"Поток {self.thread} | {self.account} | Встановлення імені користувача @{username}")
                        else:
                            logger.success(f"Thread {self.thread} | {self.account} | Set username @{username}")
                        break
                await asyncio.sleep(5)

            peer = await self.tg_client.resolve_peer('BlumCryptoBot')
            web_view = await self.tg_client.invoke(RequestAppWebView(
                peer=peer,
                app=InputBotAppShortName(bot_id=peer, short_name="app"),
                platform='android',
                write_allowed=True,
                start_param=f'{self.ref_token}'
            ))

            if with_tg is False:
                await self.tg_client.disconnect()

            data = unquote(string=web_view.url.split('#tgWebAppData=', maxsplit=1)[1].split('&tgWebAppVersion', maxsplit=1)[0])
            return data
        except:
            #logger.error   #########################
            return None


    async def run(self):
        while True:
            attempts = 3
            while attempts:
                try:
                    await self.login()
                    if self.LANG == 'RU':
                        logger.success(f"Поток {self.thread} | {self.account} | Вход выполнен!")
                    elif self.LANG == 'UA':
                        logger.success(f"Поток {self.thread} | {self.account} | Вхід виконано!")
                    else:
                        logger.success(f"Thread {self.thread} | {self.account} | Login!")
                    break
                except Exception as e:
                    if self.LANG == 'RU':
                        logger.error(f"Thread {self.thread} | {self.account} | Осталось попыток входа в систему: {attempts}, ошибка: {e}")
                    elif self.LANG == 'UA':
                        logger.error(f"Thread {self.thread} | {self.account} | Залишилося спроб входу в систему: {attempts}, помилка: {e}")
                    else:
                        logger.error(f"Thread {self.thread} | {self.account} | Left login attempts: {attempts}, error: {e}")
                    await asyncio.sleep(uniform(*settings.DELAY_RELOGIN))
                    attempts -= 1
            else:
                if self.LANG == 'RU':
                    logger.error(f"Поток {self.thread} | {self.account} | Не удалось войти")
                elif self.LANG == 'UA':
                    logger.error(f"Поток {self.thread} | {self.account} | Не вдалося увійти")
                else:
                    logger.error(f"Thread {self.thread} | {self.account} | Couldn't login")
                await self.logout()

            try:
                await self.check_daily_reward()
                await self.update_balance()
                await self.check_farming()
                await self.check_friends_balance()
                await self.elig_dogs()
                await self.check_tribe()
                await self.check_tasks()
                
                await self.play_drop_game()

            except Exception as e:
                if self.LANG == 'RU':
                    logger.error(f"Thread {self.thread} | {self.account} | Ошибка: {e}")
                elif self.LANG == 'UA':
                    logger.error(f"Thread {self.thread} | {self.account} | Помилка: {e}")
                else:
                    logger.error(f"Thread {self.thread} | {self.account} | Error: {e}")

            sleep_timer = round(random.uniform(*settings.DELAY_RESTARTING))
            if self.LANG == 'RU':
                logger.success(f"Поток {self.thread} | {self.account} | Сон: {sleep_timer} секунд...")
            elif self.LANG == 'UA':
                logger.success(f"Поток {self.thread} | {self.account} | Сон: {sleep_timer} секунд...")
            else:
                logger.success(f"Thread {self.thread} | {self.account} | Sleep: {sleep_timer} second...")
            await self.logout()
            await asyncio.sleep(sleep_timer)
