from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True)

    API_ID: int
    API_HASH: str
    
    LANG: str = 'EN'

    PLAY_GAMES: bool = False
    POINTS: list[int] = [190, 230]
    
    AUTO_TASKS: bool = True

    USE_RANDOM_DELAY_IN_RUN: bool = True
    DELAY_ACCOUNT: list[int] = [5, 10]
    DELAY_RELOGIN: list[int] = [5, 10]
    DELAY_RESTARTING: list[int] = [21600, 43200]
    
    TRIBE_CHAT_TAG: str = ''

    USE_REF: bool = True
    REF_LINK: str = 'https://t.me/BlumCryptoBot/app?startapp=ref_jYYgDsziXX'

    USE_PROXY_FROM_FILE: bool = False


settings = Settings()


