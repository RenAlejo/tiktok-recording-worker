# core/enhanced_tiktok_api.py
import json
import re
import time
import random
import asyncio
from typing import Optional, Dict, Any
from http_utils.http_client import TikTokHttpClient
from utils.enums import StatusCode, TikTokError
from utils.logger_manager import logger
from utils.custom_exceptions import (
    UserLiveException, TikTokException, LiveNotFound, IPBlockedByWAF, TikTokLiveRateLimitException
)
from database.room_cache_service import RoomCacheService
from core.tiktok_live_fallback import TikTokLiveFallback
from utils.utils import read_tiktok_live_config
from utils.user_cache import UserCache
from config.env_config import config

class TikTokAPI:
    """
    API de TikTok mejorada con características anti-detección y gestión robusta de errores
    """
    
    def __init__(self, cookies: Optional[Dict] = None):
        self.BASE_URL = config.tiktok_api_base_url
        self.WEBCAST_URL = config.tiktok_live_base_url
        
        # Cliente HTTP con proxy para API calls
        self.http_client = TikTokHttpClient(cookies=cookies)
        
        # Cache persistente en PostgreSQL
        self.db_cache = RoomCacheService()
        
        # Cache de usuarios para evitar requests innecesarios
        self.user_cache = UserCache()
        
        # Cache para optimizar requests repetitivos
        self.room_id_cache: Dict[str, tuple] = {}  # username -> (room_id, timestamp)
        self.cache_ttl = 3 * 3600  # 3 hours in seconds to match database cache TTL
        
        # Rate limiting interno
        self.last_api_call = 0
        self.min_api_interval = config.api_request_delay_ms / 1000.0  # Convert to seconds
        
        # Inicializar fallback de TikTokLive
        try:
            self.tiktok_live_fallback = TikTokLiveFallback(
                tiktok_sign_api_key=config.tiktoklive_api_key,
                session_id=config.tiktoklive_session_id
            )
            self.fallback_enabled = config.enable_fallback_api
        except Exception as e:
            logger.warning(f"TikTokLive fallback disabled: {e}")
            self.tiktok_live_fallback = None
            self.fallback_enabled = False
        
    def _rate_limit(self):
        """Aplicar rate limiting interno"""
        current_time = time.time()
        elapsed = current_time - self.last_api_call
        
        if elapsed < self.min_api_interval:
            sleep_time = self.min_api_interval - elapsed
            # Añadir jitter micro
            sleep_time += random.uniform(0, 0.2)
            time.sleep(sleep_time)
        
        self.last_api_call = time.time()
    
    def _get_cached_room_id(self, username: str) -> Optional[str]:
        """Obtiene room_id desde cache si es válido y no ha expirado el TTL de 3 horas"""
        # Verificar si el cache está habilitado
        if not config.enable_room_id_cache:
            logger.debug(f"Room ID cache disabled, skipping cache lookup for {username}")
            return None
            
        current_time = time.time()
        
        # Primero verificar cache en memoria
        if username in self.room_id_cache:
            room_id, timestamp = self.room_id_cache[username]
            if current_time - timestamp < self.cache_ttl:
                # Validar que el room_id no esté vacío o corrupto
                if room_id and room_id.strip():
                    logger.debug(f"Found valid memory cached room_id for {username}: {room_id} (age: {int(current_time - timestamp)}s)")
                    return room_id
                else:
                    # Limpiar cache corrupto
                    logger.warning(f"Removing corrupted room_id cache for {username}")
                    del self.room_id_cache[username]
            else:
                # Cache expirado, limpiar
                logger.debug(f"Memory cache expired for {username} (age: {int(current_time - timestamp)}s > {self.cache_ttl}s)")
                del self.room_id_cache[username]
        
        # Luego verificar cache en PostgreSQL con validación explícita del TTL
        cached_room_id = self.db_cache.get_cached_room_id(username)
        if cached_room_id is not None:
            # Validar que el room_id de la base de datos no esté vacío o corrupto
            if cached_room_id and cached_room_id.strip():
                # Promover a cache en memoria solo si es válido
                self.room_id_cache[username] = (cached_room_id, current_time)
                logger.debug(f"Promoted database cached room_id to memory for {username}: {cached_room_id}")
                return cached_room_id
            else:
                # Limpiar cache corrupto de la base de datos
                logger.warning(f"Removing corrupted room_id from database for {username}")
                self.db_cache.remove_cached_room_id(username)
        else:
            logger.debug(f"No valid cached room_id found for {username}")
        
        return None
    
    def _cache_room_id(self, username: str, room_id: str):
        """Guarda room_id en cache y/o base de datos según configuración"""
        current_time = time.time()
        
        # Solo procesar si el room_id no está vacío
        if room_id and room_id.strip():
            # SIEMPRE registrar en PostgreSQL como log histórico
            self.db_cache.cache_room_id(username, room_id, is_live=True)
            
            # Cache en memoria solo si está habilitado
            if config.enable_room_id_cache:
                self.room_id_cache[username] = (room_id, current_time)
                logger.info(f"Cached valid room_id for {username}: {room_id} (TTL: 3 hours)")
            else:
                logger.info(f"Logged room_id for {username}: {room_id} (cache disabled, DB log only)")
        else:
            # No cachear valores vacíos, pero limpiar cache existente si existe
            logger.warning(f"Refusing to cache empty room_id for {username}")
            if config.enable_room_id_cache and username in self.room_id_cache:
                del self.room_id_cache[username]
            # Nota: No eliminamos de DB porque queremos mantener el historial
    
    # _run_fallback_async removed - now using sync wrapper with RedisAsyncHelper

    def _get_room_id_from_api(self, user: str) -> Optional[str]:
        """
        Extrae room_id usando la API directa de TikTok
        Basado en https://www.tiktok.com/api-live/user/room/
        
        Args:
            user: Nombre de usuario (sin @)
        
        Returns:
            room_id como string, o None si no se encuentra
        """
        try:
            # Limpiar username
            if user.startswith('@'):
                user = user[1:]
            
            # URL y parámetros que funcionan
            url = "https://www.tiktok.com/api-live/user/room/"
            params = {
                "uniqueId": user,
                "sourceType": 54,
                "aid": 1988
            }
            
            # Headers optimizados
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Referer': 'https://www.tiktok.com/',
                'Origin': 'https://www.tiktok.com',
            }
            
            logger.debug(f"Requesting TikTok API for {user}: {url}")
            response = self.http_client.get_api_client().get(url, params=params, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                logger.debug(f"TikTok API response for {user}: status {response.status_code}")
                
                # Extraer room_id de la estructura conocida
                if (data.get('data') and 
                    data['data'].get('user') and 
                    data['data']['user'].get('roomId')):
                    
                    room_id = data['data']['user']['roomId']
                    logger.info(f"✅ TikTok API successful for {user}: {room_id}")
                    return room_id
                else:
                    logger.debug(f"TikTok API: No room_id found in response for {user}")
                    return None
            else:
                logger.warning(f"TikTok API returned status {response.status_code} for {user}")
                return None
                
        except Exception as e:
            logger.error(f"TikTok API error for {user}: {e}")
            return None

    def _try_fast_room_id_extraction(self, user: str) -> Optional[str]:
        """
        Método fast lane para extracción rápida de room_id usando API más liviana
        Retorna None si falla, room_id si está live, "" si no está live
        """
        try:
            self._rate_limit()
            
            # Usar endpoint más liviano para verificación inicial
            # Esto evita descargar toda la página HTML
            url = f"{self.BASE_URL}/api/user/detail/?aid=1988&unique_id={user}"
            
            response = self.http_client.get_api_client().get(
                url, 
                timeout=(config.http_connect_timeout, config.http_request_timeout)
            )
            
            if response.status_code != 200:
                logger.debug(f"Fast lane API response {response.status_code} for {user}")
                return None
                
            data = response.json()
            
            # Verificar si el usuario está live desde los datos básicos
            user_info = data.get('userInfo', {})
            if not user_info:
                logger.debug(f"Fast lane: No user info for {user}")
                return None
            
            # Buscar indicadores de live stream
            room_id = user_info.get('roomId')
            is_live_now = user_info.get('liveVerify', 0) == 1
            
            if room_id and is_live_now:
                logger.info(f"Fast lane: Found live room_id for {user}: {room_id}")
                return str(room_id)
            elif not is_live_now:
                logger.info(f"Fast lane: User {user} not live")
                return ""
            else:
                logger.debug(f"Fast lane: Inconclusive data for {user}")
                return None
                
        except Exception as e:
            logger.debug(f"Fast lane failed for {user}: {e}")
            return None
    
    def is_country_blacklisted(self) -> bool:
        """Verifica si el país está en la lista negra de TikTok"""
        try:
            self._rate_limit()
            
            response = self.http_client.get_api_client().get(
                f"{self.BASE_URL}/live",
                allow_redirects=False,
                timeout=10
            )
            
            is_blacklisted = response.status_code == StatusCode.REDIRECT
            logger.debug(f"Country blacklist check: {is_blacklisted}")
            
            return is_blacklisted
            
        except Exception as e:
            logger.warning(f"Country blacklist check failed: {e}")
            return False
    
    def is_room_alive(self, room_id: str) -> bool:
        """Verifica si una sala está activa con reintentos inteligentes"""
        if not room_id:
            return False
        
        max_retries = config.max_retries
        
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                
                url = (f"{self.WEBCAST_URL}/webcast/room/check_alive/"
                       f"?aid=1988&region=CH&room_ids={room_id}&user_is_login=true")
                
                response = self.http_client.get_api_client().get(url)
                
                # Verificar respuesta WAF
                if 'Please wait...' in response.text:
                    logger.warning("WAF detected in room check")
                    self.http_client.get_api_client().handle_waf_block()
                    
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(2, 5))
                        continue
                    else:
                        raise IPBlockedByWAF("WAF block in room check")
                
                data = response.json()
                
                if 'data' not in data or len(data['data']) == 0:
                    return False
                
                is_alive = data['data'][0].get('alive', False)
                logger.debug(f"Room {room_id} alive status: {is_alive}")
                
                return is_alive
                
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON response for room check (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(1, 3))
                    continue
                return False
                
            except Exception as e:
                logger.warning(f"Room check attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 4))
                    continue
                raise
        
        return False
    
    def get_user_from_room_id(self, room_id: str) -> str:
        """Obtiene username desde room_id con manejo robusto de errores"""
        max_retries = config.max_retries
        
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                
                url = f"{self.WEBCAST_URL}/webcast/room/info/?aid=1988&room_id={room_id}"
                response = self.http_client.get_api_client().get(url)
                
                # Manejo WAF
                if 'Please wait...' in response.text:
                    self.http_client.get_api_client().handle_waf_block()
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(3, 6))
                        continue
                    else:
                        raise IPBlockedByWAF("WAF block in user info")
                
                data = response.json()
                
                # Verificar diferentes tipos de errores
                data_str = json.dumps(data)
                
                if 'Follow the creator to watch their LIVE' in data_str:
                    raise UserLiveException(TikTokError.ACCOUNT_PRIVATE_FOLLOW)
                
                if 'This account is private' in data_str:
                    raise UserLiveException(TikTokError.ACCOUNT_PRIVATE)
                
                # Extraer display_id
                display_id = data.get("data", {}).get("owner", {}).get("display_id")
                
                if display_id is None:
                    if attempt < max_retries - 1:
                        logger.warning(f"No display_id found (attempt {attempt + 1})")
                        time.sleep(random.uniform(2, 4))
                        continue
                    else:
                        raise TikTokException(TikTokError.USERNAME_ERROR)
                
                logger.debug(f"Username from room {room_id}: {display_id}")
                return display_id
                
            except (UserLiveException, TikTokException):
                raise  # Re-raise estas excepciones específicas
            except Exception as e:
                logger.warning(f"Get user attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 5))
                    continue
                raise TikTokException(f"Failed to get user from room {room_id}: {e}")
    
    def get_room_and_user_from_url(self, live_url: str) -> tuple:
        """Extrae usuario y room_id desde URL con validación mejorada"""
        try:
            self._rate_limit()
            
            response = self.http_client.get_api_client().get(
                live_url, 
                allow_redirects=False, 
                timeout=(config.http_connect_timeout, config.http_request_timeout)
            )
            
            content = response.text
            
            if response.status_code == StatusCode.REDIRECT:
                raise UserLiveException(TikTokError.COUNTRY_BLACKLISTED)
            
            # Manejar URL móvil
            if response.status_code == StatusCode.MOVED:
                matches = re.findall(r"com/@(.*?)/live", content)
                if len(matches) < 1:
                    raise LiveNotFound(TikTokError.INVALID_TIKTOK_LIVE_URL)
                user = matches[0]
            else:
                # URL de escritorio
                match = re.match(r"https?://(?:www\.)?tiktok\.com/@([^/]+)/live", live_url)
                if not match:
                    raise LiveNotFound(TikTokError.INVALID_TIKTOK_LIVE_URL)
                user = match.group(1)
            
            # Obtener room_id
            room_id = self.get_room_id_from_user(user)
            
            logger.debug(f"Extracted from URL - User: {user}, Room: {room_id}")
            return user, room_id
            
        except (UserLiveException, LiveNotFound, TikTokException):
            raise
        except Exception as e:
            raise TikTokException(f"Failed to parse URL {live_url}: {e}")
    
    def get_room_id_from_user(self, user: str) -> str:
        """Obtiene room_id desde username con cache optimizado y reintentos"""
        logger.debug(f"Getting room_id for user: {user}")
        
        # Verificar cache solo si está habilitado
        if config.enable_room_id_cache:
            # Verificar cache de room_id válido del user_cache
            cached_room_id = self.user_cache.get_cached_room_id(user)
            if cached_room_id is not None:
                logger.info(f"Cache HIT (user_cache): Using cached room_id for {user}: {cached_room_id}")
                return cached_room_id
            
            # Verificar cache primario (memoria + PostgreSQL) con 3-hour TTL
            cached_room_id = self._get_cached_room_id(user)
            if cached_room_id is not None:
                logger.info(f"Cache HIT (primary): Using cached room_id for {user}: {cached_room_id}")
                # Cachear en user_cache para futuros accesos
                self.user_cache.cache_user_result(user, True, cached_room_id)
                return cached_room_id
        
        if config.enable_room_id_cache:
            logger.info(f"Cache MISS: No valid cached room_id found for {user}, fetching from API")
        else:
            logger.info(f"Cache DISABLED: Fetching fresh room_id for {user} from API")
        
        # PRIMARY: Usar nueva API de TikTok para room_id (rápida y confiable)
        logger.info(f"PRIMARY: Using TikTok API for room_id extraction of {user}")
        try:
            api_room_id = self._get_room_id_from_api(user)
            if api_room_id is not None:
                if api_room_id:  # Usuario está live
                    self._cache_room_id(user, api_room_id)
                    if config.enable_room_id_cache:
                        self.user_cache.cache_user_result(user, True, api_room_id)
                    logger.info(f"PRIMARY TikTok API successful for room ID of {user}: {api_room_id}")
                    return api_room_id
                else:  # Usuario no está live
                    if config.enable_room_id_cache:
                        self.user_cache.cache_user_result(user, True, None)
                    logger.info(f"PRIMARY TikTok API: User {user} not currently live")
                    return ""
            else:
                logger.info(f"PRIMARY TikTok API failed for {user}, trying fallback methods")
        except Exception as api_error:
            logger.warning(f"PRIMARY TikTok API error for {user}: {api_error}")
            logger.info(f"Falling back to alternative methods for {user}")
        
        # FALLBACK: Intentar fast lane (internal API) solo si TikTokLive falló
        logger.info(f"FALLBACK: Trying internal API (fast lane) for room_id extraction of {user}")
        fast_room_id = self._try_fast_room_id_extraction(user)
        if fast_room_id is not None:
            if fast_room_id:  # Usuario está live
                self._cache_room_id(user, fast_room_id)
                if config.enable_room_id_cache:
                    self.user_cache.cache_user_result(user, True, fast_room_id)
                logger.info(f"FALLBACK FAST LANE SUCCESS: Room ID for {user}: {fast_room_id}")
                return fast_room_id
            else:  # Usuario no está live
                if config.enable_room_id_cache:
                    self.user_cache.cache_user_result(user, True, None)
                logger.info(f"FALLBACK FAST LANE: User {user} not currently live")
                return ""
        
        logger.debug(f"Using standard extraction (fallback) for {user}")
        max_retries = config.max_retries
        
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                
                url = f'{self.BASE_URL}/@{user}/live'
                logger.debug(f"Requesting URL for {user}: {url}")
                response = self.http_client.get_api_client().get(url)
                
                logger.debug(f"Response status for {user}: {response.status_code}")
                content = response.text
                logger.debug(f"Response content length for {user}: {len(content)} chars")
                
                # Detectar WAF
                if 'Please wait...' in content:
                    logger.warning(f"WAF detected for user {user}")
                    self.http_client.get_api_client().handle_waf_block()
                    
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(5, 10))
                        continue
                    else:
                        raise IPBlockedByWAF(f"WAF block for user {user}")
                
                # Extraer SIGI_STATE
                pattern = re.compile(
                    r'<script id="SIGI_STATE" type="application/json">(.*?)</script>',
                    re.DOTALL
                )
                match = pattern.search(content)
                
                if match is None:
                    if attempt < max_retries - 1:
                        logger.warning(f"No SIGI_STATE found for {user} (attempt {attempt + 1})")
                        time.sleep(random.uniform(3, 6))
                        continue
                    else:
                        self.db_cache.mark_failed_attempt(user)
                        raise UserLiveException(TikTokError.ROOM_ID_ERROR)
                
                data = json.loads(match.group(1))
                
                # Verificar estructura de datos
                if 'LiveRoom' not in data and 'CurrentRoom' in data:
                    logger.debug(f"User {user} not live (no LiveRoom)")
                    room_id = ""
                else:
                    room_id = (data.get('LiveRoom', {})
                              .get('liveRoomUserInfo', {})
                              .get('user', {})
                              .get('roomId'))
                    
                    if room_id is None:
                        if attempt < max_retries - 1:
                            logger.warning(f"No roomId in data for {user} (attempt {attempt + 1})")
                            time.sleep(random.uniform(2, 4))
                            continue
                        else:
                            self.db_cache.mark_failed_attempt(user)
                            raise UserLiveException(TikTokError.ROOM_ID_ERROR)
                
                # Cache el resultado
                if room_id:
                    self._cache_room_id(user, room_id)
                    # También cachear en user_cache como válido solo si el cache está habilitado
                    if config.enable_room_id_cache:
                        self.user_cache.cache_user_result(user, True, room_id)
                    logger.debug(f"Room ID for {user}: {room_id}")
                    return room_id
                else:
                    # Usuario no está en vivo, pero existe - NO cachear en room_cache
                    # Solo usar user_cache para indicar que el usuario existe pero no está live
                    logger.info(f"User {user} exists but is not currently live")
                    # No cachear un room_id vacío, pero marcar el usuario como válido en user_cache solo si está habilitado
                    if config.enable_room_id_cache:
                        self.user_cache.cache_user_result(user, True, None)
                    return ""  # Retornar string vacío para indicar "no live"
                
            except (UserLiveException, IPBlockedByWAF) as e:
                self.db_cache.mark_failed_attempt(user)
                # Determinar tipo de error para cache inteligente solo si el cache está habilitado
                if config.enable_room_id_cache:
                    error_type = "USER_LIVE_EXCEPTION" if isinstance(e, UserLiveException) else "IP_BLOCKED"
                    self.user_cache.cache_user_result(user, False, error_type=error_type)
                raise
            except json.JSONDecodeError as e:
                logger.warning(f"JSON decode error for {user} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 5))
                    continue
                self.db_cache.mark_failed_attempt(user)
                # Error de parse podría ser usuario inexistente o problema temporal
                if config.enable_room_id_cache:
                    self.user_cache.cache_user_result(user, False, error_type="JSON_DECODE_ERROR")
                raise UserLiveException(TikTokError.ROOM_ID_ERROR)
            except Exception as e:
                error_message = str(e)
                logger.warning(f"Room ID attempt {attempt + 1} failed for {user}: {error_message}")
                
                # Verificar tipos específicos de errores
                is_protocol_error = "HTTP/2 stream 0 was not closed cleanly: PROTOCOL_ERROR" in error_message
                is_connection_error = any(conn_err in error_message.lower() for conn_err in [
                    "connection reset by peer", 
                    "remote end closed connection",
                    "connection refused",
                    "timeout"
                ])
                is_user_not_live = any(not_live_pattern in error_message.lower() for not_live_pattern in [
                    "not hosting a live stream", 
                    "not currently live",
                    "user is not live",
                    "never been in live"
                ])
                
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(3, 6))
                    continue
                
                # En el último intento, el primary TikTokLive ya fue probado
                # No hay más fallbacks disponibles para room_id extraction
                
                self.db_cache.mark_failed_attempt(user)
                # Cachear el fallo general si el cache está habilitado
                if config.enable_room_id_cache:
                    if is_protocol_error or is_connection_error:
                        self.user_cache.cache_user_result(user, False, error_type="CONNECTION_PROTOCOL_ERROR")
                    elif is_user_not_live:
                        self.user_cache.cache_user_result(user, False, error_type="NOT_CURRENTLY_LIVE")
                    else:
                        self.user_cache.cache_user_result(user, False, error_type="GENERAL_ERROR")
                raise
        
        self.db_cache.mark_failed_attempt(user)
        raise UserLiveException(TikTokError.ROOM_ID_ERROR)
    
    def get_live_url(self, room_id: str, user: str) -> str:
        """Obtiene URL de stream con algoritmo de calidad mejorado y fallback a TikTokLive"""
        max_retries = config.max_retries
        
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                
                url = f"{self.WEBCAST_URL}/webcast/room/info/?aid=1988&room_id={room_id}"
                response = self.http_client.get_api_client().get(url)
                
                data = response.json()
                
                # Verificar cuenta privada
                if 'This account is private' in json.dumps(data):
                    raise UserLiveException(TikTokError.ACCOUNT_PRIVATE)
                
                stream_url = data.get('data', {}).get('stream_url', {})
                
                # Intentar obtener URL moderna primero
                sdk_data_str = (stream_url.get('live_core_sdk_data', {})
                               .get('pull_data', {})
                               .get('stream_data'))
                
                if sdk_data_str:
                    best_url = self._extract_best_quality_url(stream_url, sdk_data_str)
                    if best_url:
                        logger.debug(f"Using modern stream URL for room {room_id}")
                        return best_url
                
                # Fallback a URLs legacy
                logger.warning("No SDK stream data found, using legacy URLs")
                legacy_url = self._get_legacy_stream_url(stream_url)
                
                if not legacy_url:
                    if data.get('status_code') == 4003110:
                        raise UserLiveException(TikTokError.LIVE_RESTRICTION)
                    
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(2, 4))
                        continue
                    else:
                        raise LiveNotFound(TikTokError.RETRIEVE_LIVE_URL)
                
                return legacy_url
                
            except (UserLiveException, LiveNotFound) as e:
                rate_limit_raised = False
                if config.enable_fallback_api and self.fallback_enabled and self.tiktok_live_fallback and user:
                    logger.info(f"Trying TikTokLive fallback for user '{user}' (room {room_id}) due to: {e}")
                    try:
                        # Use sync wrapper to avoid event loop issues
                        room_info = self.tiktok_live_fallback.get_room_info_sync(user)
                        
                        if room_info and self.tiktok_live_fallback.is_live(room_info):
                            stream_url = self.tiktok_live_fallback.extract_stream_urls(room_info)
                            if stream_url:
                                logger.info(f"TikTokLive fallback successful for {user}")
                                return stream_url
                            else:
                                logger.warning(f"TikTokLive fallback: No stream URL found for {user}")
                    except TikTokLiveRateLimitException as rate_limit_ex:
                        logger.error(f"TikTokLive rate limit reached for {user} - propagating rate limit exception")
                        rate_limit_raised = True
                        raise rate_limit_ex
                    except Exception as fallback_error:
                        logger.error(f"TikTokLive fallback failed: {fallback_error}")
                
                if not rate_limit_raised:
                    raise
            except TikTokLiveRateLimitException:
                raise
            except Exception as e:
                logger.warning(f"Live URL attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 5))
                    continue
                raise
        
        raise LiveNotFound(TikTokError.RETRIEVE_LIVE_URL)
    
    def _extract_best_quality_url(self, stream_url: Dict, sdk_data_str: str) -> Optional[str]:
        """Extrae la mejor calidad disponible del SDK data"""
        try:
            sdk_data = json.loads(sdk_data_str).get('data', {})
            qualities = (stream_url.get('live_core_sdk_data', {})
                        .get('pull_data', {})
                        .get('options', {})
                        .get('qualities', []))
            
            if not qualities:
                return None
            
            # Mapear niveles de calidad
            level_map = {q['sdk_key']: q['level'] for q in qualities}
            
            # Encontrar la mejor calidad
            best_level = -1
            best_flv = None
            
            for sdk_key, entry in sdk_data.items():
                level = level_map.get(sdk_key, -1)
                stream_main = entry.get('main', {})
                flv_url = stream_main.get('flv')
                
                if flv_url and level > best_level:
                    best_level = level
                    best_flv = flv_url
            
            return best_flv
            
        except Exception as e:
            logger.warning(f"Error extracting best quality URL: {e}")
            return None
    
    def _get_legacy_stream_url(self, stream_url: Dict) -> Optional[str]:
        """Obtiene URL de stream usando método legacy"""
        flv_urls = stream_url.get('flv_pull_url', {})
        
        # Prioridades de calidad
        quality_priorities = ['FULL_HD1', 'HD1', 'SD2', 'SD1']
        
        for quality in quality_priorities:
            url = flv_urls.get(quality)
            if url:
                logger.debug(f"Using legacy {quality} quality")
                return url
        
        # Último recurso: RTMP
        rtmp_url = stream_url.get('rtmp_pull_url', '')
        return rtmp_url if rtmp_url else None
    
    def download_live_stream(self, live_url: str):
        """Generator para descargar stream usando requests directo sin proxy"""
        session = None
        try:
            import requests
            
            # Headers específicos para descarga de stream (no para API)
            # Estos headers son los que un navegador enviaría para descargar video
            stream_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'identity',  # Importante: no gzip para streams
                'Connection': 'keep-alive',
                'Range': 'bytes=0-',  # Importante para streaming
                'Referer': 'https://www.tiktok.com/',
                'Origin': 'https://www.tiktok.com',
                'Sec-Fetch-Dest': 'video',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'cross-site'
            }
            
            # Crear sesión completamente nueva SIN proxy para streaming
            session = requests.Session()
            session.headers.update(stream_headers)
            
            # Timeouts más permisivos para streaming (no para API)
            # Connect timeout corto, read timeout largo para streams
            with session.get(live_url, stream=True, timeout=(10, 60)) as response:
                response.raise_for_status()
                
                # Chunk size más grande para streaming eficiente
                for chunk in response.iter_content(chunk_size=16384):
                    if chunk:
                        yield chunk
                
        except Exception as e:
            logger.error(f"Stream download error: {e}")
            raise
        finally:
            # CRÍTICO: Cerrar la sesión para evitar conexiones CLOSE_WAIT
            if session:
                try:
                    session.close()
                    logger.debug("HTTP session closed for stream download")
                except Exception as cleanup_error:
                    logger.warning(f"Error closing HTTP session in stream download: {cleanup_error}")
    
    def close(self):
        """Cierra el cliente HTTP"""
        if self.http_client:
            self.http_client.close()
    
    def clear_expired_cache(self):
        """Limpia entradas expiradas del cache de room_ids"""
        current_time = time.time()
        expired_users = []
        
        # Revisar cache en memoria
        for username, (room_id, timestamp) in list(self.room_id_cache.items()):
            if current_time - timestamp >= self.cache_ttl:
                expired_users.append(username)
                del self.room_id_cache[username]
        
        # Limpiar cache de base de datos
        self.db_cache.cleanup_old_entries()
        
        if expired_users:
            logger.info(f"Cleared {len(expired_users)} expired room_id cache entries: {expired_users}")
        else:
            logger.debug("No expired room_id cache entries found")
    
    def get_cache_stats(self):
        """Obtiene estadísticas del cache de room_ids"""
        current_time = time.time()
        memory_entries = len(self.room_id_cache)
        
        # Contar entradas válidas vs expiradas en memoria
        valid_entries = 0
        expired_entries = 0
        
        for username, (room_id, timestamp) in self.room_id_cache.items():
            if current_time - timestamp < self.cache_ttl:
                valid_entries += 1
            else:
                expired_entries += 1
        
        return {
            'memory_cache_total': memory_entries,
            'memory_cache_valid': valid_entries,
            'memory_cache_expired': expired_entries,
            'cache_ttl_hours': self.cache_ttl / 3600
        }
    
    def clear_cache(self):
        """Limpia todo el cache de room_ids"""
        self.room_id_cache.clear()
        self.db_cache.cleanup_old_entries()
        logger.info("All room ID cache cleared")