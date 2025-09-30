# print banner
from utils.utils import banner

banner()

# check and install dependencies
from utils.dependencies import check_and_install_dependencies

check_and_install_dependencies()

from check_updates import check_updates

import sys
import os
import asyncio

from utils.args_handler import validate_and_parse_args
from utils.utils import read_cookies
from utils.logger_manager import logger

from core.tiktok_recorder import TikTokRecorder
from utils.enums import TikTokError
from utils.custom_exceptions import LiveNotFound, ArgsParseError, \
    UserLiveException, IPBlockedByWAF, TikTokException

from bot.tiktok_bot import TikTokBot

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def main():
    bot = TikTokBot()
    try:
        await bot.run()  # Este método ahora maneja todo el ciclo de vida del bot
    except KeyboardInterrupt:
        logger.info("Bot detenido por el usuario")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot detenido por el usuario")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
