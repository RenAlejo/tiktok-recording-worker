from enum import Enum, IntEnum, auto


class Regex(Enum):

    def __str__(self):
        return str(self.value)

    IS_TIKTOK_LIVE = r".*www\.tiktok\.com.*|.*vm\.tiktok\.com.*"


class TimeOut(IntEnum):
    """
    Enumeration that defines timeout values.
    """

    def __mul__(self, operator):
        return self.value * operator

    ONE_MINUTE = 60
    AUTOMATIC_MODE = 5
    CONNECTION_CLOSED = 2


class StatusCode(IntEnum):
    OK = 200
    REDIRECT = 302
    MOVED = 301


class Mode(IntEnum):
    """
    Enumeration that represents the recording modes.
    """
    MANUAL = 0
    AUTOMATIC = 1


class Error(Enum):
    """
    Enumeration that contains possible errors while using TikTok-Live-Recorder.
    """

    def __str__(self):
        return str(self.value)



    CONNECTION_CLOSED = "Connection broken by the server."
    CONNECTION_CLOSED_AUTOMATIC = f"{CONNECTION_CLOSED}. Try again after delay of {TimeOut.CONNECTION_CLOSED} minutes"


class TikTokError(Enum):
    """
    Enumeration that contains possible errors of TikTok
    """

    def __str__(self):
        return str(self.value)

    COUNTRY_BLACKLISTED = 'Captcha required or country blocked. '

    COUNTRY_BLACKLISTED_AUTO_MODE = \
        'Automatic mode is available only in unblocked countries. '

    ACCOUNT_PRIVATE = 'Account is private, login required. '
    
    ACCOUNT_PRIVATE_FOLLOW = 'This account is private. Follow the creator to access their LIVE.'

    LIVE_RESTRICTION = 'Live is private, login required. '

    USERNAME_ERROR = 'Username / RoomID not found or the user has never been in live.'

    ROOM_ID_ERROR = 'Error extracting RoomID'

    USER_NEVER_BEEN_LIVE = "The user has never hosted a live stream on TikTok."

    USER_NOT_CURRENTLY_LIVE = "The user is not hosting a live stream at the moment."

    RETRIEVE_LIVE_URL = 'Unable to retrieve live streaming url. Please try again later.'

    INVALID_TIKTOK_LIVE_URL = 'The provided URL is not a valid TikTok live stream.'

    WAF_BLOCKED = 'Your IP is blocked by TikTok WAF. Please change your IP address.'



class Info(Enum):
    """
    Enumeration that defines the version number and the banner message.
    """

    def __str__(self):
        return str(self.value)

    def __iter__(self):
        return iter(self.value)

    NEW_FEATURES = [
        "Bug fixes",
    ]

    VERSION = 1.0
    RELEASE_DATE = "2025-05-25"
    CONTRIBUTORS = """ 
@Michele0303: Tiktok-Live-Api features
@AlejoRendo - Tiktok-Live-Recorder and Telegram bot features""" 
    BANNER = fr"""

 /$$$$$$$                      /$$       /$$                     /$$$$$$$              /$$    
| $$__  $$                    | $$      |__/                    | $$__  $$            | $$    
| $$  \ $$  /$$$$$$   /$$$$$$$| $$       /$$ /$$    /$$ /$$$$$$ | $$  \ $$  /$$$$$$  /$$$$$$  
| $$$$$$$/ /$$__  $$ /$$_____/| $$      | $$|  $$  /$$//$$__  $$| $$$$$$$  /$$__  $$|_  $$_/  
| $$__  $$| $$$$$$$$| $$      | $$      | $$ \  $$/$$/| $$$$$$$$| $$__  $$| $$  \ $$  | $$    
| $$  \ $$| $$_____/| $$      | $$      | $$  \  $$$/ | $$_____/| $$  \ $$| $$  | $$  | $$ /$$
| $$  | $$|  $$$$$$$|  $$$$$$$| $$$$$$$$| $$   \  $/  |  $$$$$$$| $$$$$$$/|  $$$$$$/  |  $$$$/
|__/  |__/ \_______/ \_______/|________/|__/    \_/    \_______/|_______/  \______/    \___/  
                                                                                              
                                                                                                                                                                                                                                        
V{VERSION} - {RELEASE_DATE}
CONTRIBUTORS:{CONTRIBUTORS}
"""

class UploadError(Enum):
    """Errores específicos para el proceso de carga"""
    FILE_NOT_FOUND = auto()
    FILE_TOO_LARGE = auto()
    FILE_CORRUPTED = auto()
    FILE_INVALID = auto()
    CONNECTION_ERROR = auto()
    TIMEOUT_ERROR = auto()
    TELEGRAM_SERVER_ERROR = auto()
    FLOOD_WAIT_ERROR = auto()
    NETWORK_ERROR = auto()
    UNKNOWN_ERROR = auto()
    
    @classmethod
    def from_exception(cls, exception: Exception) -> 'UploadError':
        """Convierte una excepción en un UploadError"""
        error_map = {
            'FileNotFoundError': cls.FILE_NOT_FOUND,
            'FileTooLargeError': cls.FILE_TOO_LARGE,
            'CorruptedFileError': cls.FILE_CORRUPTED,
            'InvalidFileError': cls.FILE_INVALID,
            'ConnectionError': cls.CONNECTION_ERROR,
            'TimeoutError': cls.TIMEOUT_ERROR,
            'TelegramServerError': cls.TELEGRAM_SERVER_ERROR,
            'FloodWaitError': cls.FLOOD_WAIT_ERROR,
            'NetworkError': cls.NETWORK_ERROR
        }
        return error_map.get(type(exception).__name__, cls.UNKNOWN_ERROR)
