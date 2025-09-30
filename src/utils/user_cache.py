import time
import threading
from typing import Dict, Set, Optional
from dataclasses import dataclass
from utils.logger_manager import logger

@dataclass
class CacheEntry:
    timestamp: float
    is_valid_user: bool
    room_id: Optional[str] = None
    error_type: Optional[str] = None
    attempt_count: int = 1
    last_fallback_attempt: Optional[float] = None

class UserCache:
    """
    Cache para usuarios de TikTok para evitar requests repetitivos a usuarios inexistentes
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
        self.cache: Dict[str, CacheEntry] = {}
        self.cache_lock = threading.Lock()
        self.cache_duration = 60 * 60  # 1 hora por defecto
        self.invalid_users: Set[str] = set()  # Lista negra de usuarios inexistentes
        
        # Cargar configuración
        self._load_config()
        
        # Iniciar thread de limpieza
        self._start_cleanup_thread()
    
    def _load_config(self):
        """Carga la configuración del cache"""
        try:
            from utils.utils import read_proxy_optimization_config
            config = read_proxy_optimization_config()
            optimization = config.get('proxy_optimization', {})
            
            if optimization.get('user_cache_enabled', True):
                self.cache_duration = optimization.get('user_cache_duration_minutes', 60) * 60
            else:
                self.cache_duration = 0  # Deshabilitado
                
            logger.info(f"User cache initialized with {self.cache_duration}s duration")
        except Exception as e:
            logger.error(f"Error loading user cache config: {e}")
    
    def is_enabled(self) -> bool:
        """Verifica si el cache está habilitado"""
        return self.cache_duration > 0
    
    def is_user_known_invalid(self, username: str) -> bool:
        """Verifica si un usuario está en la lista negra con lógica híbrida inteligente"""
        if not self.is_enabled():
            return False
            
        with self.cache_lock:
            current_time = time.time()
            
            # Verificar cache general con lógica híbrida
            if username in self.cache:
                entry = self.cache[username]
                
                # Si es un usuario válido, no está en lista negra
                if entry.is_valid_user:
                    return False
                
                # Calcular TTL dinámico basado en intentos y tipo de error
                dynamic_ttl = self._calculate_dynamic_ttl(entry)
                
                # Si el cache no ha expirado según el TTL dinámico
                if current_time - entry.timestamp < dynamic_ttl:
                    # No bloquear usuarios que pueden usar fallback
                    if self._can_use_fallback(entry):
                        return False
                    return True
                else:
                    # Cache expirado, remover entrada para permitir re-intento
                    del self.cache[username]
                    self.invalid_users.discard(username)
        
        return False
    
    def _calculate_dynamic_ttl(self, entry: CacheEntry) -> float:
        """Calcula TTL dinámico basado en intentos y tipo de error"""
        base_ttl = 1800  # 30 minutos base
        
        # TTL muy corto para usuarios que no están en vivo (pueden estar en vivo pronto)
        if entry.error_type == 'NOT_CURRENTLY_LIVE':
            return 300  # Solo 5 minutos para usuarios que no están en vivo
        
        # TTL más corto para errores de protocolo/conexión (podrían ser temporales)
        if entry.error_type == 'CONNECTION_PROTOCOL_ERROR':
            return base_ttl // 2  # 15 minutos para errores de protocolo
        
        # TTL escalonado por número de intentos para otros errores
        if entry.attempt_count == 1:
            return base_ttl  # 30 minutos
        elif entry.attempt_count == 2:
            return base_ttl * 4  # 2 horas
        elif entry.attempt_count == 3:
            return base_ttl * 12  # 6 horas
        else:
            return base_ttl * 48  # 24 horas máximo
    
    def _can_use_fallback(self, entry: CacheEntry) -> bool:
        """Determina si un usuario puede usar fallback basado en el tipo de error"""
        if not entry.error_type:
            return True
        
        # Tipos de error que pueden beneficiarse del fallback
        fallback_compatible_errors = [
            'PRIVATE_ACCOUNT',
            'RESTRICTED_ACCOUNT', 
            'AUTH_REQUIRED',
            'USER_LIVE_EXCEPTION',
            'ROOM_ID_ERROR',
            'NOT_CURRENTLY_LIVE'  # Los usuarios que no están en vivo pueden intentar fallback y estar en vivo más tarde
        ]
        
        # Errores que definitivamente no pueden usar fallback
        non_fallback_errors = [
            'USER_NOT_FOUND',
            'ACCOUNT_BANNED',
            'INVALID_USERNAME',
            'CONNECTION_PROTOCOL_ERROR'  # Errores de protocolo/conexión no dan info del usuario
            # REMOVIDO 'NOT_CURRENTLY_LIVE' - Los usuarios que no están en vivo pueden estar en vivo más tarde
        ]
        
        if entry.error_type in non_fallback_errors:
            return False
            
        if entry.error_type in fallback_compatible_errors:
            # Permitir fallback si no se ha intentado recientemente
            if entry.last_fallback_attempt is None:
                return True
            # Permitir un fallback cada 2 horas
            return (time.time() - entry.last_fallback_attempt) > 7200
        
        # Para errores desconocidos, permitir fallback
        return True
    
    def get_cached_room_id(self, username: str) -> Optional[str]:
        """Obtiene el room_id del cache si está disponible y válido"""
        if not self.is_enabled():
            return None
            
        with self.cache_lock:
            if username in self.cache:
                entry = self.cache[username]
                if time.time() - entry.timestamp < self.cache_duration:
                    if entry.is_valid_user:
                        return entry.room_id
        
        return None
    
    def cache_user_result(self, username: str, is_valid: bool, room_id: Optional[str] = None, error_type: Optional[str] = None):
        """Cachea el resultado de una consulta de usuario con estrategia híbrida"""
        if not self.is_enabled():
            return
            
        with self.cache_lock:
            current_time = time.time()
            
            # Si existe una entrada previa, incrementar contador de intentos
            attempt_count = 1
            last_fallback_attempt = None
            
            if username in self.cache:
                existing_entry = self.cache[username]
                if not is_valid and not existing_entry.is_valid_user:
                    # Solo incrementar si sigue siendo inválido
                    attempt_count = existing_entry.attempt_count + 1
                    last_fallback_attempt = existing_entry.last_fallback_attempt
            
            entry = CacheEntry(
                timestamp=current_time,
                is_valid_user=is_valid,
                room_id=room_id,
                error_type=error_type,
                attempt_count=attempt_count,
                last_fallback_attempt=last_fallback_attempt
            )
            
            self.cache[username] = entry
            
            # Si es inválido, agregarlo a la lista negra (con lógica híbrida)
            if not is_valid:
                # Solo agregar a invalid_users si no puede usar fallback
                if not self._can_use_fallback(entry):
                    self.invalid_users.add(username)
                    logger.debug(f"Added {username} to invalid users cache (attempt {attempt_count}, error: {error_type})")
                else:
                    logger.debug(f"Cached {username} as invalid but allowing fallback (attempt {attempt_count}, error: {error_type})")
            else:
                # Si ahora es válido, removerlo de la lista negra
                self.invalid_users.discard(username)
                logger.debug(f"Cached {username} as valid user")
    
    def mark_user_invalid(self, username: str, error_type: Optional[str] = None):
        """Marca un usuario como inválido"""
        self.cache_user_result(username, False, error_type=error_type)
    
    def mark_fallback_attempt(self, username: str):
        """Marca que se intentó usar fallback para un usuario"""
        if not self.is_enabled():
            return
            
        with self.cache_lock:
            if username in self.cache:
                entry = self.cache[username]
                entry.last_fallback_attempt = time.time()
                logger.debug(f"Marked fallback attempt for {username}")
    
    def should_try_fallback(self, username: str) -> bool:
        """Determina si se debe intentar fallback para un usuario"""
        if not self.is_enabled():
            return True
            
        with self.cache_lock:
            if username in self.cache:
                entry = self.cache[username]
                if not entry.is_valid_user:
                    return self._can_use_fallback(entry)
        
        return True
    
    def clear_user_cache(self, username: str):
        """Limpia el cache de un usuario específico"""
        with self.cache_lock:
            self.cache.pop(username, None)
            self.invalid_users.discard(username)
    
    def clear_all_cache(self):
        """Limpia todo el cache"""
        with self.cache_lock:
            self.cache.clear()
            self.invalid_users.clear()
        logger.info("User cache cleared")
    
    def _start_cleanup_thread(self):
        """Inicia el hilo de limpieza del cache"""
        def cleanup_worker():
            while True:
                try:
                    time.sleep(1800)  # Cada 30 minutos
                    self._cleanup_expired_entries()
                except Exception as e:
                    logger.error(f"Cache cleanup error: {e}")
        
        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cleanup_thread.start()
        logger.info("User cache cleanup thread started")
    
    def _cleanup_expired_entries(self):
        """Limpia entradas expiradas del cache"""
        current_time = time.time()
        expired_users = []
        
        with self.cache_lock:
            for username, entry in self.cache.items():
                if current_time - entry.timestamp > self.cache_duration:
                    expired_users.append(username)
            
            for username in expired_users:
                del self.cache[username]
                self.invalid_users.discard(username)
        
        if expired_users:
            logger.debug(f"Cleaned up {len(expired_users)} expired cache entries")
    
    def get_cache_stats(self) -> Dict:
        """Obtiene estadísticas del cache"""
        with self.cache_lock:
            total_entries = len(self.cache)
            invalid_entries = len(self.invalid_users)
            valid_entries = sum(1 for entry in self.cache.values() if entry.is_valid_user)
            
            return {
                'total_entries': total_entries,
                'valid_users': valid_entries,
                'invalid_users': invalid_entries,
                'cache_duration': self.cache_duration
            }