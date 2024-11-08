# BLUM

[![Static Badge](https://img.shields.io/badge/Telegram-BOT-Link?style=for-the-badge&logo=Telegram&logoColor=white&logoSize=auto&color=blue)](https://t.me/BlumCryptoBot/app?startapp=ref_jYYgDsziXX)

[![Static Badge](https://img.shields.io/badge/My_Telegram_Сhannel-@CryptoCats__tg-Link?style=for-the-badge&logo=Telegram&logoColor=white&logoSize=auto&color=blue)](https://t.me/CryptoCats_tg)

## Recommendation before use

# 🔥🔥 Use PYTHON 3.10-3.11 🔥🔥

[![Static Badge](https://img.shields.io/badge/README_in_Ukrainian_available-README_%D0%A3%D0%BA%D1%80%D0%B0%D1%97%D0%BD%D1%81%D1%8C%D0%BA%D0%BE%D1%8E_%D0%BC%D0%BE%D0%B2%D0%BE%D1%8E-blue.svg?style=for-the-badge&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxMjAwIiBoZWlnaHQ9IjgwMCI+DQo8cmVjdCB3aWR0aD0iMTIwMCIgaGVpZ2h0PSI4MDAiIGZpbGw9IiMwMDU3QjciLz4NCjxyZWN0IHdpZHRoPSIxMjAwIiBoZWlnaHQ9IjQwMCIgeT0iNDAwIiBmaWxsPSIjRkZENzAwIi8+DQo8L3N2Zz4=)](README-UA.md)
[![Static Badge](https://img.shields.io/badge/README_in_russian_available-README_%D0%BD%D0%B0_%D1%80%D1%83%D1%81%D1%81%D0%BA%D0%BE%D0%BC_%D1%8F%D0%B7%D1%8B%D0%BA%D0%B5-blue?style=for-the-badge)](README-RU.md)


## Functionality
| Functional                                                     | Supported |
|----------------------------------------------------------------|:---------:|
| Multithreading                                                 |     ✅     |
| Binding proxy to a session                                     |     ✅     |
| Binding User-Agent to a session                                |     ✅     |
| Automatic account registration with your referral code         |     ✅     |
| Auto-daily reward		           							     |     ✅     |
| Auto-game with a choice of random points		                 |     ✅     |
| Completion of tasks		              		     	         |     ✅     |
| Random sleep time between accounts                             |     ✅     |
| Support pyrogram .session                                      |     ✅     |

## [Settings](https://github.com/CatSnowdrop/BLUM/blob/main/.env-example/)
|           Settings            |                                       Description                                     |
|:-----------------------------:|:-------------------------------------------------------------------------------------:|
|         **API_ID**            |        Your Telegram API ID (integer)                                                 |
|         **API_HASH**          |        Your Telegram API Hash (string)                                                |
|          **LANG**             |        Interface language (EN / RU / UA)                                              |
|        **PLAY_GAMES**         |        Play games or just start farming (True / False)                                |
|          **POINTS**           |        Points per game (eg, [190, 230])                          					    |
|        **AUTO_TASKS**         |        Automatically perform tasks (True / False)                                     |
|  **USE_RANDOM_DELAY_IN_RUN**  |        Use random startup delay (True / False)                                        |
|      **DELAY_ACCOUNT**        |        Random startup delay (eg, [0, 15])                                             |
|      **DELAY_RELOGIN**        |        Delay after unsuccessful login attempt (eg, [0, 15])                           |
|    **DELAY_RESTARTING**       |        Delay before restarting the program (eg, [21600, 43200])                       |
|     **TRIBE_CHAT_TAG**        |        Your tribe telegram tag for auto join            					            |
|         **USE_REF**           |        Whether to use a refreral link (True / False)                                  |
|         **REF_LINK**          |        Referral link				                                                    |
|  **USE_PROXY_FROM_FILE**      |        Use proxy from `bot/config/proxies.txt` (True / False). Otherwise, the proxy from the `sessions/accounts.json` file is used. |


## Quick Start 📚
Windows: To install libraries and run bot - open run.bat

Linux: To install libraries and run bot - open run.sh

## Prerequisites
Before you begin, make sure you have the following installed:
- [Python](https://www.python.org/downloads/) **version 3.10-3.11**

## Obtaining API Keys
1. Go to my.telegram.org and log in using your phone number.
2. Select "API development tools" and fill out the form to register a new application.

## Installation
You can download the [**repository**](https://github.com/CatSnowdrop/BLUM) by cloning it to your system and installing the necessary dependencies:
```shell
git clone https://github.com/CatSnowdrop/BLUM.git
cd BLUM
```

Then you can do automatic installation by typing:

Windows:
```shell
run.bat
```

Linux:
```shell
run.sh
```


# Linux manual installation
```shell
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
python3 main.py
```

You can also use arguments for quick start, for example:
```shell
~/BLUM >>> python3 main.py --action (1/2)
# Or
~/BLUM >>> python3 main.py -a (1/2)

# 1 - Start soft
# 2 - Create sessions
```

# Windows manual installation
```shell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

You can also use arguments for quick start, for example:
```shell
~/BLUM >>> python main.py --action (1/2)
# Or
~/BLUM >>> python main.py -a (1/2)

# 1 - Start soft
# 2 - Create sessions
```
