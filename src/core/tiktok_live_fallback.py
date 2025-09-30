import os
import asyncio
from typing import Optional, Dict, Any
from TikTokLive import TikTokLiveClient
from TikTokLive.client.web.web_settings import WebDefaults
from utils.logger_manager import logger
from utils.custom_exceptions import LiveNotFound, UserLiveException, TikTokLiveRateLimitException
from utils.enums import TikTokError
from httpx import Proxy
import random
from utils.utils import read_proxy_config, read_socks5_proxy_config
from services.redis_helper import RedisAsyncHelper

class TikTokLiveFallback:
    """
    Cliente fallback usando TikTokLive para obtener URLs de stream
    cuando el TikTokAPI principal falla debido a restricciones de autenticación
    """
    
    def __init__(self, tiktok_sign_api_key: str, session_id: str):
        self.tiktok_sign_api_key = tiktok_sign_api_key
        self.session_id = session_id
        
        # Configurar variable de entorno para autorizar session ID
        os.environ['WHITELIST_AUTHENTICATED_SESSION_ID_HOST'] = 'tiktok.eulerstream.com'
        
        # Configurar API key
        WebDefaults.tiktok_sign_api_key = tiktok_sign_api_key
        
        # Proxy rotation tracking
        self._used_proxy_combinations = set()
        self._max_retries_per_combination = 2

    def _get_unused_proxy_combination(self):
        """
        Obtiene una combinación de proxies HTTP+SOCKS5 que no haya sido usada recientemente
        """
        http_proxies = read_proxy_config().get("proxies", [])
        socks5_proxies = read_socks5_proxy_config()
        
        if not http_proxies and not socks5_proxies:
            return None, None
            
        # Generar todas las combinaciones posibles
        combinations = []
        for http_proxy in http_proxies:
            for socks5_proxy in socks5_proxies:
                combo_key = f"{http_proxy.get('host')}:{http_proxy.get('port')}+{socks5_proxy.get('host')}:{socks5_proxy.get('port')}"
                combinations.append((http_proxy, socks5_proxy, combo_key))
        
        # Si no hay SOCKS5, usar solo HTTP
        if not socks5_proxies and http_proxies:
            for http_proxy in http_proxies:
                combo_key = f"{http_proxy.get('host')}:{http_proxy.get('port')}+none"
                combinations.append((http_proxy, None, combo_key))
        
        # Si no hay HTTP, usar solo SOCKS5
        if not http_proxies and socks5_proxies:
            for socks5_proxy in socks5_proxies:
                combo_key = f"none+{socks5_proxy.get('host')}:{socks5_proxy.get('port')}"
                combinations.append((None, socks5_proxy, combo_key))
        
        # Filtrar combinaciones no usadas
        unused_combinations = [combo for combo in combinations if combo[2] not in self._used_proxy_combinations]
        
        # Si todas fueron usadas, resetear y usar cualquiera
        if not unused_combinations:
            logger.info("🔄 All proxy combinations used, resetting rotation cache")
            self._used_proxy_combinations.clear()
            unused_combinations = combinations
        
        if unused_combinations:
            selected = random.choice(unused_combinations)
            http_proxy, socks5_proxy, combo_key = selected
            self._used_proxy_combinations.add(combo_key)
            logger.info(f"🌐 Selected proxy combination: {combo_key}")
            return http_proxy, socks5_proxy
        
        return None, None
    
    def get_room_info_sync(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Synchronous wrapper for get_room_info that handles async operations safely
        Uses RedisAsyncHelper to execute async operations from sync context
        """
        try:
            # Use RedisAsyncHelper to safely run async operation from sync context
            result = RedisAsyncHelper.run_async_safe(
                self.get_room_info,
                username,
                timeout=30
            )
            return result
            
        except Exception as e:
            logger.error(f"Error in sync wrapper for get_room_info: {e}")
            return None

    def _get_random_http_proxy(self):
        proxies = read_proxy_config().get("proxies", [])
        if not proxies:
            return None
        return random.choice(proxies)

    def _get_random_socks5_proxy(self):
        proxies = read_socks5_proxy_config()
        if not proxies:
            return None
        return random.choice(proxies)

    async def get_room_info(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene información de la sala usando TikTokLive con rotación de proxies
        """
        max_attempts = 3
        
        for attempt in range(max_attempts):
            client = None
            try:
                # Obtener una combinación no usada de proxies
                http_proxy, socks5_proxy = self._get_unused_proxy_combination()
                
                # Configurar web_proxy (HTTP)
                web_proxy = None
                if http_proxy:
                    host = http_proxy["host"]
                    port = http_proxy["port"]
                    user = http_proxy.get("username")
                    pwd = http_proxy.get("password")
                    web_proxy = Proxy(f"http://{host}:{port}", auth=(user, pwd) if user and pwd else None)

                # Configurar ws_proxy (SOCKS5)
                ws_proxy = None
                if socks5_proxy:
                    host = socks5_proxy["host"]
                    port = socks5_proxy["port"]
                    user = socks5_proxy.get("username")
                    pwd = socks5_proxy.get("password")
                    ws_proxy = Proxy(
                        f"{socks5_proxy['protocol']}://{socks5_proxy['host']}:{socks5_proxy['port']}",
                        auth=(user, pwd) if user and pwd else None
                    )

                client = TikTokLiveClient(
                    unique_id=f"@{username}",
                    web_proxy=web_proxy,
                    ws_proxy=ws_proxy
                )
                
                # Configurar session ID
                if self.session_id and self.session_id != "TU_SESSION_ID_AQUI":
                    client.web.set_session(self.session_id, "useast1a")
                    logger.debug(f"Session ID configurado para fallback: {self.session_id[:10]}...")
                
                # Conectar y obtener información de la sala
                await client.start(fetch_room_info=True)
                
                # Obtener room_info
                if hasattr(client, 'room_info') and client.room_info:
                    room_info = client.room_info
                    logger.info(f"TikTokLive fallback: Room info obtenido para {username} (attempt {attempt + 1})")
                    return room_info
                else:
                    logger.warning(f"TikTokLive fallback: No se pudo obtener room_info para {username} (attempt {attempt + 1})")
                    # Continuar al siguiente intento, el cleanup se hace en finally
                    if attempt == max_attempts - 1:
                        return None
                    # No hacer continue aquí para que pase por finally
                    
            except Exception as e:
                error_msg = str(e)
                logger.error(f"TikTokLive fallback error para {username} (attempt {attempt + 1}): {e}")
                
                # Detectar errores de rate limit - NO reintentar, lanzar excepción inmediatamente
                if "rate_limit" in error_msg.lower() or "too many connections" in error_msg.lower() or "reached daily limit" in error_msg.lower():
                    logger.error(f"❌ TikTokLive API rate limit reached for {username}")
                    raise TikTokLiveRateLimitException(f"TikTokLive API rate limit exceeded for user {username}")
                
                # Para otros errores, no reintentar
                return None
            finally:
                # Asegurar que el cliente se cierre correctamente
                try:
                    if 'client' in locals() and client:
                        await client.stop()
                except Exception as cleanup_error:
                    logger.debug(f"Client cleanup error: {cleanup_error}")
                    pass
        
        logger.error(f"TikTokLive fallback: Failed to get room info for {username} after {max_attempts} attempts")
        return None
    
    def extract_stream_urls(self, room_info: Dict[str, Any]) -> Optional[str]:
        """
        Extrae las URLs de stream del room_info de TikTokLive
        Retorna la mejor URL disponible
        """
        try:
            stream_url = room_info.get('stream_url', {})
            if not stream_url:
                logger.warning("TikTokLive fallback: No stream_url encontrado en room_info")
                return None
            
            # Intentar obtener URL FLV (preferida para grabación)
            flv_pull_url = stream_url.get('flv_pull_url', {})
            if flv_pull_url:
                # Prioridades de calidad para FLV
                quality_priorities = ['FULL_HD1', 'HD1', 'SD2', 'SD1']
                
                for quality in quality_priorities:
                    url = flv_pull_url.get(quality)
                    if url:
                        logger.info(f"TikTokLive fallback: Usando FLV URL con calidad {quality}")
                        return url
            
            # Fallback a HLS
            hls_pull_url = stream_url.get('hls_pull_url')
            if hls_pull_url:
                logger.info("TikTokLive fallback: Usando HLS URL")
                return hls_pull_url
            
            # Último recurso: RTMP
            rtmp_pull_url = stream_url.get('rtmp_pull_url')
            if rtmp_pull_url:
                logger.info("TikTokLive fallback: Usando RTMP URL")
                return rtmp_pull_url
            
            logger.warning("TikTokLive fallback: No se encontraron URLs de stream válidas")
            return None
            
        except Exception as e:
            logger.error(f"TikTokLive fallback: Error extrayendo URLs: {e}")
            return None
    
    def extract_room_id(self, room_info: Dict[str, Any]) -> Optional[str]:
        """
        Extrae el room_id del room_info de TikTokLive
        """
        try:
            room_id = room_info.get('id')
            if room_id:
                logger.info(f"TikTokLive fallback: Room ID extraído: {room_id}")
                return str(room_id)
            return None
        except Exception as e:
            logger.error(f"TikTokLive fallback: Error extrayendo room_id: {e}")
            return None
    
    def is_live(self, room_info: Dict[str, Any]) -> bool:
        """
        Verifica si el usuario está en vivo basado en el room_info
        """
        try:
            status = room_info.get('status', 0)
            is_live = status == 2  # Status 2 indica que está en vivo
            logger.info(f"TikTokLive fallback: Status del usuario: {status} (en vivo: {is_live})")
            return is_live
        except Exception as e:
            logger.error(f"TikTokLive fallback: Error verificando status: {e}")
            return False 