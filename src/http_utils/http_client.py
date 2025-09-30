# http_utils/enhanced_http_client.py
import random
import time
import threading
from typing import Dict, List, Optional, Any
import requests
from utils.enums import StatusCode
from utils.logger_manager import logger
from http_utils.proxy_manager import ProxyManager
from config.env_config import config

class HttpClient:
    """
    Cliente HTTP mejorado con características anti-detección:
    - Rotación de User-Agents
    - Gestión dinámica de cookies
    - Jitter en requests
    - Fingerprinting TLS mejorado
    - Calentamiento de sesión
    """
    
    USER_AGENTS = [
        # Chrome Windows - versiones más conservadoras
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        
        # Chrome macOS
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        
        # Firefox - versiones estables
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:108.0) Gecko/20100101 Firefox/108.0",
        
        # Edge
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.61",
    ]
    
    TT_TARGET_IDC_VALUES = [
        "useast2a"
        # "euwest1a", "eucentral1a", "apnortheast1a", "apsoutheast1a"
    ]
    
    def __init__(self, use_proxy: bool = None, cookies: Optional[Dict] = None):
        # Usar configuración desde env_config si no se especifica
        if use_proxy is None:
            use_proxy = config.enable_proxy
        
        # Cargar configuración de optimización
        self._load_optimization_config()
        
        # Log sobre modo determinístico
        if config.deterministic_recording:
            logger.info("HTTP Client running in DETERMINISTIC mode for cross-machine consistency")
        else:
            logger.info("HTTP Client running in RANDOM mode (may vary between machines)")
        
        self.proxy_manager = ProxyManager() if (use_proxy and self.proxy_enabled) else None
        self.session = None
        self.cookies = cookies or {}
        self.session_lock = threading.Lock()
        self.last_request_time = 0
        self.min_request_interval = config.api_request_delay_ms / 1000.0
        self.max_request_interval = self.min_request_interval * 3
        
        self._create_session()
    
    def _load_optimization_config(self):
        """Carga la configuración de optimización de proxy"""
        try:
            from utils.utils import read_proxy_optimization_config
            proxy_config = read_proxy_optimization_config()
            
            self.proxy_enabled = config.enable_proxy
            optimization = proxy_config.get('proxy_optimization', {})
            
            self.max_retries = config.max_retries
            self.reduce_retries_for_protocol_errors = not config.fallback_on_protocol_error
            self.protocol_error_max_retries = max(1, config.max_retries // 2)
            self.fail_fast_errors = optimization.get('fail_fast_errors', [])
            
            logger.info(f"HTTP optimization loaded: proxy_enabled={self.proxy_enabled}, max_retries={self.max_retries}")
        except Exception as e:
            # Configuración por defecto desde env_config
            self.proxy_enabled = config.enable_proxy
            self.max_retries = config.max_retries
            self.reduce_retries_for_protocol_errors = not config.fallback_on_protocol_error
            self.protocol_error_max_retries = max(1, config.max_retries // 2)
            self.fail_fast_errors = []
            logger.warning(f"Using default HTTP optimization config: {e}")
    
    def _create_session(self):
        """Crea una nueva sesión con configuración anti-detección"""
        with self.session_lock:
            # Usar requests estándar - más simple y sin problemas de compatibilidad
            self.session = requests.Session()
            logger.debug("Using standard requests session")
            
            # Aplicar headers optimizados
            self.session.headers.update(self._get_random_headers())
            
            # Aplicar cookies
            if self.cookies:
                self.session.cookies.update(self._process_cookies(self.cookies))
            
            # Configurar proxy si está habilitado
            if self.proxy_manager:
                self._setup_proxy()
    
    def _get_random_headers(self) -> Dict[str, str]:
        """Genera headers aleatorios para simular navegador real"""
        # Si está habilitado el modo determinístico, usar configuración fija
        if config.deterministic_recording:
            user_agent_index = config.fixed_user_agent_index
            user_agent = self.USER_AGENTS[user_agent_index % len(self.USER_AGENTS)]
        else:
            user_agent = random.choice(self.USER_AGENTS)
        
        # Detectar tipo de navegador para headers específicos
        is_chrome = "Chrome" in user_agent
        is_firefox = "Firefox" in user_agent
        
        headers = {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }
        
        # Temporalmente comentado para diagnosticar problemas
        # if is_chrome:
        #     headers.update({
        #         "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        #         "Sec-Ch-Ua-Mobile": "?0",
        #         "Sec-Ch-Ua-Platform": '"Windows"' if "Windows" in user_agent else '"macOS"',
        #         "Sec-Fetch-Site": "same-origin",
        #         "Sec-Fetch-Mode": "navigate",
        #         "Sec-Fetch-User": "?1",
        #         "Sec-Fetch-Dest": "document"
        #     })
        # elif is_firefox:
        #     headers.update({
        #         "Sec-Fetch-Site": "same-origin",
        #         "Sec-Fetch-Mode": "navigate",
        #         "Sec-Fetch-User": "?1",
        #         "Sec-Fetch-Dest": "document"
        #     })
        
        return headers
    
    def _process_cookies(self, cookies: Dict) -> Dict:
        """Procesa y optimiza cookies dinámicamente"""
        processed_cookies = cookies.copy()
        
        # Gestión dinámica de tt-target-idc
        if 'tt-target-idc' in processed_cookies:
            # Si está habilitado el modo determinístico, usar valor fijo
            if config.deterministic_recording:
                processed_cookies['tt-target-idc'] = self.TT_TARGET_IDC_VALUES[0]
            else:
                processed_cookies['tt-target-idc'] = random.choice(self.TT_TARGET_IDC_VALUES)
        
        return processed_cookies
    
    def _setup_proxy(self):
        """Configura el proxy actual"""
        if not self.proxy_manager:
            return
            
        current_proxy = self.proxy_manager.get_current_proxy()
        if current_proxy:
            proxy_dict = self.proxy_manager.format_proxy_for_requests(current_proxy)
            self.session.proxies.update(proxy_dict)
            logger.debug(f"Using proxy: {current_proxy.host}")
    
    def _apply_jitter(self):
        """Aplica jitter (aleatoriedad) entre requests"""
        current_time = time.time()
        elapsed_since_last = current_time - self.last_request_time
        
        # Si está habilitado modo determinístico, usar intervalo fijo
        if config.deterministic_recording and config.disable_random_delays:
            jitter_interval = self.min_request_interval
        else:
            # Calcular intervalo aleatorio
            jitter_interval = random.uniform(self.min_request_interval, self.max_request_interval)
        
        if elapsed_since_last < jitter_interval:
            sleep_time = jitter_interval - elapsed_since_last
            # Añadir variación adicional solo si no está en modo determinístico
            if not (config.deterministic_recording and config.disable_random_delays):
                sleep_time += random.uniform(0, 0.5)
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _warm_session(self):
        """Calienta la sesión haciendo request a la página principal"""
        try:
            logger.debug("Warming up session...")
            response = self.session.get(
                "https://www.tiktok.com",
                timeout=10,
                allow_redirects=True
            )
            
            # Actualizar cookies con las recibidas
            if response.cookies:
                self.session.cookies.update(response.cookies)
                logger.debug("Session cookies updated from warm-up")
                
        except Exception as e:
            logger.warning(f"Session warm-up failed: {e}")
    
    def _is_fail_fast_error(self, error_message: str) -> bool:
        """Verifica si es un error que debe fallar rápido"""
        error_str = str(error_message).lower()
        for fail_fast_pattern in self.fail_fast_errors:
            if fail_fast_pattern.lower() in error_str:
                return True
        return False
    
    def _should_reduce_retries(self, error_message: str) -> bool:
        """Verifica si debemos reducir los reintentos para este error"""
        if not self.reduce_retries_for_protocol_errors:
            return False
        
        error_str = str(error_message).lower()
        protocol_errors = [
            "http/2 stream 0 was not closed cleanly: protocol_error",
            "connection reset by peer",
            "remote end closed connection without response"
        ]
        
        for pattern in protocol_errors:
            if pattern in error_str:
                return True
        return False

    def _should_rotate_proxy(self, error_message: str) -> bool:
        """Identifica errores que requieren rotación de proxy para aprovechar los 100 proxies disponibles"""
        error_str = str(error_message).lower()
        
        # Errores de conexión que justifican rotación inmediata de proxy
        connection_errors = [
            "connection timeout",
            "connection timed out",
            "read timeout",
            "read timed out",
            "connection refused",
            "connection reset",
            "connection aborted",
            "connection error",
            "connection failed",
            "timeout",
            "timed out", 
            "network is unreachable",
            "no route to host",
            "connection broken",
            "proxy error",
            "502 bad gateway",
            "503 service unavailable",
            "504 gateway timeout",
            "522",  # Cloudflare timeout
            "captcha required",
            "country blocked",
            "waf",
            "blocked",
            "forbidden",
            "access denied",
            "too many requests",
            "rate limit",
            "http/2 stream",
            "protocol_error",
            "ssl",
            "certificate",
            "handshake",
            "tunnel connection failed"
        ]
        
        # Verificar si algún patrón de error coincide
        for pattern in connection_errors:
            if pattern in error_str:
                return True
        
        return False

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Realiza una request con todas las protecciones anti-detección optimizadas"""
        max_retries = self.max_retries
        
        for attempt in range(max_retries):
            try:
                # Aplicar jitter
                self._apply_jitter()
                
                # Calentar sesión ocasionalmente (reducido para ahorrar requests)
                # Solo en modo no determinístico
                if not config.deterministic_recording and random.random() < 0.05:  # 5% de probabilidad
                    self._warm_session()
                
                # Rotar headers ocasionalmente (reducido para consistencia)
                # Solo en modo no determinístico
                if not config.deterministic_recording and random.random() < 0.02:  # 2% de probabilidad
                    self.session.headers.update(self._get_random_headers())
                
                # Aplicar timeouts estandarizados si está habilitado el modo determinístico
                if config.deterministic_recording and config.standardize_timeouts:
                    if 'timeout' not in kwargs:
                        # Usar timeouts consistentes para garantizar mismo comportamiento
                        kwargs['timeout'] = (config.http_connect_timeout, config.http_request_timeout)
                
                # Realizar request
                response = self.session.request(method, url, **kwargs)
                
                # Registrar éxito si usamos proxy
                if self.proxy_manager:
                    current_proxy = self.proxy_manager.get_current_proxy()
                    if current_proxy:
                        response_time = response.elapsed if isinstance(response.elapsed, float) else response.elapsed.total_seconds()
                        self.proxy_manager.mark_proxy_success(current_proxy, response_time)
                
                return response
                
            except Exception as e:
                error_message = str(e)
                logger.warning(f"Request attempt {attempt + 1} failed: {error_message}")
                
                # Verificar si es un error de fail-fast
                if self._is_fail_fast_error(error_message):
                    logger.info(f"Fail-fast error detected, stopping retries: {error_message}")
                    raise e
                
                # Verificar si debemos reducir reintentos para errores de protocolo
                if self._should_reduce_retries(error_message):
                    if attempt >= self.protocol_error_max_retries - 1:
                        logger.info(f"Protocol error retry limit reached ({self.protocol_error_max_retries}): {error_message}")
                        raise e
                
                # Rotar proxy solo en errores específicos y después de varios intentos
                if (self.proxy_manager and 
                    self._should_rotate_proxy(error_message) and 
                    attempt >= 2):  # Solo después de 2 intentos fallidos
                    current_proxy = self.proxy_manager.get_current_proxy()
                    if current_proxy:
                        self.proxy_manager.mark_proxy_error(current_proxy, f"connection_error_attempt_{attempt + 1}")
                        self._setup_proxy()  # Cambiar proxy
                        logger.info(f"Rotated proxy due to connection error: {current_proxy.host} -> {self.proxy_manager.get_current_proxy().host if self.proxy_manager.get_current_proxy() else 'None'}")
                
                # Reintentar con nueva sesión en el último intento (solo si no es protocolo error)
                if attempt == max_retries - 1 and not self._should_reduce_retries(error_message):
                    logger.info("Creating new session for final retry")
                    self._create_session()
                
                # Backoff más conservador para requests de API
                # Usar intervalos determinísticos si está habilitado
                if config.deterministic_recording and config.consistent_retry_intervals:
                    if self._should_rotate_proxy(error_message):
                        time.sleep(1.5)  # Intervalo fijo
                    elif self._should_reduce_retries(error_message):
                        time.sleep(2.0)  # Intervalo fijo
                    else:
                        time.sleep(3.0)  # Intervalo fijo
                else:
                    # Backoff aleatorio original
                    if self._should_rotate_proxy(error_message):
                        time.sleep(min(1 * (attempt + 1), 3))  # Máximo 3 segundos, más conservador
                    elif self._should_reduce_retries(error_message):
                        time.sleep(min(2 ** attempt, 4))  # Máximo 4 segundos
                    else:
                        time.sleep(min(2 ** attempt, 8))  # Máximo 8 segundos para otros errores
        
        raise Exception(f"All {max_retries} attempts failed for {url}")
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """GET request con protecciones"""
        return self.request('GET', url, **kwargs)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """POST request con protecciones"""
        return self.request('POST', url, **kwargs)
    
    def handle_waf_block(self):
        """Maneja bloqueos WAF rotando cookies y proxy"""
        logger.warning("WAF block detected, rotating credentials...")
        
        # Rotar tt-target-idc
        if config.deterministic_recording:
            new_idc = self.TT_TARGET_IDC_VALUES[0]  # Usar valor fijo
        else:
            new_idc = random.choice(self.TT_TARGET_IDC_VALUES)
        self.session.cookies['tt-target-idc'] = new_idc
        
        # Cambiar proxy si está disponible
        if self.proxy_manager:
            current_proxy = self.proxy_manager.get_current_proxy()
            if current_proxy:
                self.proxy_manager.mark_proxy_error(current_proxy, "waf_block")
                self._setup_proxy()
        
        # Crear nueva sesión
        self._create_session()
        
        logger.info("Credentials rotated after WAF block")
    
    def close(self):
        """Cierra la sesión"""
        if self.session:
            self.session.close()


class StandardHttpClient:
    """
    Cliente HTTP estándar usando requests para descargas de stream
    """
    
    def __init__(self, use_proxy: bool = False, cookies: Optional[Dict] = None):
        import requests
        self.session = requests.Session()
        self.cookies = cookies or {}
        
        # Configurar headers básicos
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        })
        
        # Aplicar cookies si están disponibles
        if self.cookies:
            self.session.cookies.update(self.cookies)
    
    def get(self, url: str, **kwargs):
        """GET request usando requests estándar"""
        return self.session.get(url, **kwargs)
    
    def close(self):
        """Cierra la sesión"""
        if self.session:
            self.session.close()


class TikTokHttpClient:
    """
    Cliente HTTP específico para TikTok con gestión separada para API y descarga
    """
    
    def __init__(self, cookies: Optional[Dict] = None):
        # Usar configuración desde env_config
        use_proxy_for_api = config.enable_proxy
        
        # Cliente con/sin proxy para API calls basado en configuración
        self.api_client = HttpClient(use_proxy=use_proxy_for_api, cookies=cookies)
        
        # Cliente sin proxy para descargas (usa requests estándar)
        self.download_client = StandardHttpClient(use_proxy=False, cookies=cookies)
        
    def get_api_client(self) -> HttpClient:
        """Obtiene el cliente para llamadas API (con proxy)"""
        return self.api_client
    
    def get_download_client(self) -> StandardHttpClient:
        """Obtiene el cliente para descargas (sin proxy)"""
        return self.download_client
    
    def close(self):
        """Cierra ambos clientes"""
        self.api_client.close()
        self.download_client.close()