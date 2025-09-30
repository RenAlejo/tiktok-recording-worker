 # proxy_manager.py
import random
import time
import threading
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum
from utils.logger_manager import logger
import requests
from config.env_config import config

class ProxyStatus(Enum):
    ACTIVE = "active"
    BLOCKED = "blocked"
    ERROR = "error"
    TESTING = "testing"

@dataclass
class ProxyInfo:
    host: str
    port: int
    username: str = None
    password: str = None
    status: ProxyStatus = ProxyStatus.ACTIVE
    last_used: float = 0
    error_count: int = 0
    success_count: int = 0
    response_time: float = 0

class ProxyManager:
    """
    Gestor global de proxies residenciales con rotación inteligente y estado persistente
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
            
        self._initialized = True
        self.proxies: List[ProxyInfo] = []
        self.current_proxy_index = 0
        self.rotation_lock = threading.Lock()
        self.test_timeout = config.http_connect_timeout
        self.max_error_count = 5  # Aumentar tolerancia de errores por proxy
        self.rotation_interval = 2400  # 40 minutos
        self.last_rotation = time.time()
        
        # Cargar proxies desde configuración
        self._load_proxies()
        
        # Iniciar thread de mantenimiento
        self._start_maintenance_thread()
    
    def _load_proxies(self):
        """Carga la lista de proxies desde configuración"""
        try:
            from utils.utils import read_proxy_config
            proxy_config = read_proxy_config()
            
            for proxy_data in proxy_config.get('proxies', []):
                proxy = ProxyInfo(
                    host=proxy_data['host'],
                    port=proxy_data['port'],
                    username=proxy_data.get('username'),
                    password=proxy_data.get('password')
                )
                self.proxies.append(proxy)
                
            logger.info(f"Loaded {len(self.proxies)} proxies")
            
        except Exception as e:
            logger.error(f"Error loading proxy configuration: {e}")
            # Configuración de emergencia con proxies de ejemplo
            self._load_fallback_proxies()
    
    def _load_fallback_proxies(self):
        """Configuración de respaldo de proxies"""
        fallback_proxies = [
            {"host": "gate.decodo.com", "port": 10100, "username": "spf3yb9j9d", "password": "imFbRD8jqq2ip3V=1m"},
            {"host": "gate.decodo.com", "port": 10099, "username": "spf3yb9j9d", "password": "imFbRD8jqq2ip3V=1m"},
        ]
        
        for proxy_data in fallback_proxies:
            proxy = ProxyInfo(**proxy_data)
            self.proxies.append(proxy)
    
    def get_current_proxy(self) -> Optional[ProxyInfo]:
        """Obtiene el proxy actual con rotación automática"""
        if not self.proxies:
            return None
            
        with self.rotation_lock:
            # Verificar si es necesario rotar
            current_time = time.time()
            if (current_time - self.last_rotation) > self.rotation_interval:
                self._rotate_proxy()
                self.last_rotation = current_time
            
            # Obtener proxy activo
            attempts = 0
            while attempts < len(self.proxies):
                proxy = self.proxies[self.current_proxy_index]
                
                if proxy.status == ProxyStatus.ACTIVE and proxy.error_count < self.max_error_count:
                    proxy.last_used = current_time
                    return proxy
                
                # Si el proxy actual no está disponible, rotar
                self._rotate_proxy()
                attempts += 1
            
            logger.warning("No active proxies available")
            return None
    
    def _rotate_proxy(self):
        """Rota al siguiente proxy disponible, saltando proxies bloqueados"""
        if len(self.proxies) <= 1:
            return
        
        original_index = self.current_proxy_index
        attempts = 0
        max_attempts = len(self.proxies)
        
        while attempts < max_attempts:
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)
            current_proxy = self.proxies[self.current_proxy_index]
            
            # Buscar un proxy que no esté bloqueado
            if current_proxy.status == ProxyStatus.ACTIVE and current_proxy.error_count < self.max_error_count:
                logger.info(f"Rotated to proxy: {current_proxy.host}:{current_proxy.port} (errors: {current_proxy.error_count})")
                return
            
            attempts += 1
        
        # Si todos los proxies están bloqueados, usar el menos problemático
        best_proxy = min(self.proxies, key=lambda p: p.error_count)
        self.current_proxy_index = self.proxies.index(best_proxy)
        logger.warning(f"All proxies have issues, using least problematic: {best_proxy.host}:{best_proxy.port} (errors: {best_proxy.error_count})")

    def _rotate_proxy_smart(self, avoid_host: str = None):
        """Rota al siguiente proxy disponible, evitando hosts específicos"""
        if len(self.proxies) <= 1:
            return
        
        original_index = self.current_proxy_index
        attempts = 0
        max_attempts = len(self.proxies)
        
        while attempts < max_attempts:
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)
            current_proxy = self.proxies[self.current_proxy_index]
            
            # Saltar proxies del host problemático si se especifica
            if avoid_host and current_proxy.host == avoid_host:
                attempts += 1
                continue
            
            # Buscar un proxy que no esté bloqueado
            if current_proxy.status == ProxyStatus.ACTIVE and current_proxy.error_count < self.max_error_count:
                logger.info(f"Smart rotated to proxy: {current_proxy.host}:{current_proxy.port} (errors: {current_proxy.error_count}, avoided: {avoid_host})")
                return
            
            attempts += 1
        
        # Si no se encuentra uno bueno evitando el host, usar la rotación normal
        logger.warning(f"Could not avoid host {avoid_host}, using normal rotation")
        self._rotate_proxy()
    
    def mark_proxy_error(self, proxy: ProxyInfo, error_type: str = "unknown"):
        """Marca un proxy como con error"""
        with self.rotation_lock:
            proxy.error_count += 1
            
            if proxy.error_count >= self.max_error_count:
                proxy.status = ProxyStatus.BLOCKED
                logger.warning(f"Proxy {proxy.host} marked as blocked after {proxy.error_count} errors")
                
                # Rotar inmediatamente si es el proxy actual
                if self.proxies[self.current_proxy_index] == proxy:
                    self._rotate_proxy_smart(avoid_host=proxy.host)
    
    def mark_proxy_success(self, proxy: ProxyInfo, response_time: float):
        """Marca un proxy como exitoso"""
        proxy.success_count += 1
        proxy.response_time = response_time
        proxy.error_count = max(0, proxy.error_count - 1)  # Reducir errores gradualmente
        
        if proxy.status == ProxyStatus.BLOCKED and proxy.error_count == 0:
            proxy.status = ProxyStatus.ACTIVE
            logger.info(f"Proxy {proxy.host} restored to active status")
    
    def test_proxy(self, proxy: ProxyInfo) -> bool:
        """Prueba un proxy específico"""
        try:
            proxy.status = ProxyStatus.TESTING
            start_time = time.time()
            
            proxy_dict = {
                'http': f'http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}',
                'https': f'http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}'
            }
            
            response = requests.get(
                'https://httpbin.org/ip',
                proxies=proxy_dict,
                timeout=self.test_timeout
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                self.mark_proxy_success(proxy, response_time)
                proxy.status = ProxyStatus.ACTIVE
                return True
            else:
                self.mark_proxy_error(proxy, "http_error")
                return False
                
        except Exception as e:
            logger.error(f"Proxy test failed for {proxy.host}: {e}")
            self.mark_proxy_error(proxy, str(e))
            proxy.status = ProxyStatus.ERROR
            return False
    
    def _start_maintenance_thread(self):
        """Inicia el hilo de mantenimiento de proxies"""
        def maintenance_worker():
            while True:
                try:
                    time.sleep(2400)  # Cada 40 minutos
                    self._perform_maintenance()
                except Exception as e:
                    logger.error(f"Maintenance thread error: {e}")
        
        maintenance_thread = threading.Thread(target=maintenance_worker, daemon=True)
        maintenance_thread.start()
        logger.info("Proxy maintenance thread started")
    
    def _perform_maintenance(self):
        """Realiza mantenimiento de proxies"""
        logger.info("Performing proxy maintenance...")
        
        for proxy in self.proxies:
            if proxy.status in [ProxyStatus.BLOCKED, ProxyStatus.ERROR]:
                # Reintentar proxies bloqueados/con error cada cierto tiempo
                if time.time() - proxy.last_used > 1800:  # 30 minutos
                    logger.info(f"Retesting proxy: {proxy.host}")
                    self.test_proxy(proxy)
    
    def get_proxy_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de los proxies"""
        active_count = sum(1 for p in self.proxies if p.status == ProxyStatus.ACTIVE)
        blocked_count = sum(1 for p in self.proxies if p.status == ProxyStatus.BLOCKED)
        error_count = sum(1 for p in self.proxies if p.status == ProxyStatus.ERROR)
        
        return {
            'total_proxies': len(self.proxies),
            'active': active_count,
            'blocked': blocked_count,
            'error': error_count,
            'current_proxy': self.proxies[self.current_proxy_index].host if self.proxies else None
        }
    
    def format_proxy_for_requests(self, proxy: ProxyInfo) -> Dict[str, str]:
        """Formatea el proxy para uso con requests"""
        if proxy.username and proxy.password:
            proxy_url = f'http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}'
        else:
            proxy_url = f'http://{proxy.host}:{proxy.port}'
            
        return {
            'http': proxy_url,
            'https': proxy_url
        }