"""
Helper functions para ejecutar operaciones Redis async desde contextos síncronos
"""

import asyncio
import threading
import time
from typing import Any, Callable, Optional
from utils.logger_manager import logger


class RedisAsyncHelper:
    """
    Helper para ejecutar operaciones Redis async desde código síncrono
    Maneja event loops de forma robusta
    """

    @staticmethod
    def run_async_safe(async_func: Callable, *args, timeout: int = 30, **kwargs) -> Any:
        """
        Ejecuta una función async de forma segura desde contexto síncrono

        Args:
            async_func: Función async a ejecutar
            *args: Argumentos posicionales
            timeout: Timeout en segundos
            **kwargs: Argumentos con nombre

        Returns:
            Resultado de la función async o None si falla
        """
        try:
            # Verificar si hay un loop ejecutándose
            try:
                asyncio.get_running_loop()
                # Hay un loop ejecutándose, usar thread separado
                return RedisAsyncHelper._run_in_thread(async_func, timeout, *args, **kwargs)
            except RuntimeError:
                # No hay loop, podemos crear uno nuevo
                return asyncio.run(async_func(*args, **kwargs))

        except asyncio.TimeoutError as e:
            logger.warning(f"Timeout executing async function {async_func.__name__} after {timeout}s: {e}")
            return None
        except Exception as e:
            logger.error(f"Error executing async function {async_func.__name__}: {e}")
            return None

    @staticmethod
    def _run_in_thread(async_func: Callable, timeout: int, *args, **kwargs) -> Any:
        """
        Ejecuta función async en thread separado con su propio event loop
        """
        result = [None]
        error = [None]

        def worker():
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result[0] = loop.run_until_complete(async_func(*args, **kwargs))
                finally:
                    loop.close()
            except Exception as e:
                error[0] = e

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        if thread.is_alive():
            logger.error(f"Timeout executing {async_func.__name__} after {timeout}s")
            return None

        if error[0]:
            logger.error(f"Error in thread executing {async_func.__name__}: {error[0]}")
            return None

        return result[0]