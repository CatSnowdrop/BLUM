class InvalidSession(BaseException):
    ...

class NeedReLoginError(Exception):
    pass

class NeedRefreshTokenError(Exception):
    pass