from utils.enums import TikTokError


class TikTokException(Exception):
    def __init__(self, message):
        super().__init__(message)


class UserLiveException(Exception):
    def __init__(self, message):
        super().__init__(message)


class IPBlockedByWAF(Exception):
    def __init__(self, message=TikTokError.WAF_BLOCKED):
        super().__init__(message)


class LiveNotFound(Exception):
    pass


class ArgsParseError(Exception):
    pass


class TikTokLiveRateLimitException(Exception):
    """
    Excepción específica para cuando se alcanza el rate limit de TikTokLive API
    Se usa para distinguir entre errores normales y límites de API de lives privados
    """
    def __init__(self, message="TikTokLive API rate limit reached"):
        super().__init__(message)
