"""
Redis service for persistent recording subscription management.
Provides transactional operations for recording sessions and subscriber tracking.
"""

import redis
import json
import time
import uuid
import asyncio
import threading
from typing import List, Dict, Optional, Any

from utils.logger_manager import logger
from config.env_config import config


class RecordingRedisService:
    """
    Singleton Redis service for recording subscription persistence.
    
    Provides atomic operations for:
    - Recording session management
    - Subscriber tracking with delivery confirmation
    - Complete cleanup after all deliveries confirmed
    """
    
    _instance: Optional['RecordingRedisService'] = None
    _initialized: bool = False
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        with self._lock:
            if self._initialized:
                return
            
            try:
                # Redis connection with connection pool
                self.redis = redis.from_url(
                    config.redis_url, 
                    decode_responses=True,
                    max_connections=config.redis_max_connections,
                    retry_on_timeout=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                
                # Configuration
                self.recording_ttl = config.recording_subscription_ttl_days * 24 * 3600  # TTL in seconds
                
                # Test connection
                self.redis.ping()
                logger.info(f" ✅ Redis connection established")
                
                self._initialized = True
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize Redis service: {e}")
                raise
    
    @classmethod
    def get_instance(cls) -> 'RecordingRedisService':
        """Obtiene la instancia singleton del servicio Redis"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def generate_recording_id(self, username: str) -> str:
        """Genera ID único para grabación: username_timestamp_uuid"""
        timestamp = int(time.time())
        unique_id = str(uuid.uuid4())[:8]
        return f"{username}_{timestamp}_{unique_id}"
    
    async def create_recording_session(self, username: str, user_id: int, chat_id: int) -> str:
        """
        Crea nueva sesión de grabación de forma atómica
        
        Args:
            username: Nombre del usuario en TikTok
            user_id: ID del usuario de Telegram que inicia
            chat_id: ID del chat donde se inicia
            
        Returns:
            recording_id generado para la sesión
        """
        try:
            recording_id = self.generate_recording_id(username)
            session_key = f"recording:{username}:{int(time.time())}"
            subscribers_key = f"subscribers:{recording_id}"
            primary_key = f"primary_delivery:{recording_id}"
            
            # Operación atómica con pipeline
            with self.redis.pipeline() as pipe:
                pipe.multi()
                
                # 1. Crear sesión de grabación
                pipe.hset(session_key, mapping={
                    "recording_id": recording_id,
                    "username": username,
                    "primary_user_id": str(user_id),
                    "primary_chat_id": str(chat_id),
                    "status": "active",
                    "processing_status": "recording",  # recording -> processing -> processed -> failed
                    "start_time": str(int(time.time())),
                    "fragment_count": "1",
                    "current_fragment": "1",
                    "is_fragmented": "false",
                    "all_fragments_completed": "false",
                    "last_activity": str(int(time.time()))
                })
                
                # 2. Inicializar estructura de suscriptores
                pipe.hset(subscribers_key, "initialized", "true")
                
                # 3. Agregar a cola de pendientes
                pipe.zadd("pending_deliveries", {recording_id: time.time()})
                
                # 4. Inicializar estado de entrega primaria como pendiente
                pipe.hset(primary_key, mapping={
                    "upload_status": "pending",
                    "delivery_status": "pending", 
                    "upload_attempts": "0",
                    "created_at": str(int(time.time()))
                })
                
                # 5. Configurar TTL para auto-cleanup
                pipe.expire(session_key, self.recording_ttl)
                pipe.expire(subscribers_key, self.recording_ttl)
                pipe.expire(primary_key, self.recording_ttl)
                pipe.expire("pending_deliveries", self.recording_ttl)
                
                pipe.execute()
            
            logger.info(f"✅ Created recording session: {recording_id} for {username}")
            return recording_id
            
        except Exception as e:
            logger.error(f"❌ Error creating recording session for {username}: {e}")
            raise
    
    async def add_subscription(self, recording_id: str, user_id: int, chat_id: int) -> bool:
        """
        Añade suscriptor a grabación existente de forma atómica
        
        Args:
            recording_id: ID de la grabación a la que suscribirse
            user_id: ID del usuario que se suscribe
            chat_id: ID del chat del suscriptor
            
        Returns:
            True si se añadió exitosamente, False si falló
        """
        try:
            subscribers_key = f"subscribers:{recording_id}"
            user_key = f"user_{user_id}"
            user_subs_key = f"user_subs:{user_id}"
            
            # Verificar que la grabación existe y está activa
            if not self.redis.zscore("pending_deliveries", recording_id):
                logger.warning(f"⚠️ Recording {recording_id} not found in pending deliveries")
                return False
            
            # Verificar si el usuario ya está suscrito
            existing_sub = self.redis.hget(subscribers_key, user_key)
            if existing_sub:
                logger.info(f"👤 User {user_id} already subscribed to {recording_id}")
                return True
            
            # Operación atómica de suscripción
            with self.redis.pipeline() as pipe:
                pipe.multi()
                
                # 1. Añadir suscriptor con metadata
                pipe.hset(subscribers_key, user_key, json.dumps({
                    "user_id": str(user_id),
                    "chat_id": str(chat_id),
                    "subscribed_at": str(int(time.time())),
                    "delivery_status": "pending",
                    "retry_count": "0"
                }))
                
                # 2. Añadir a lista de suscripciones del usuario
                pipe.sadd(user_subs_key, recording_id)
                
                # 3. Actualizar TTL
                pipe.expire(subscribers_key, self.recording_ttl)
                pipe.expire(user_subs_key, self.recording_ttl)
                
                pipe.execute()
            
            logger.info(f"✅ Added subscriber {user_id} to recording {recording_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error adding subscription for user {user_id} to {recording_id}: {e}")
            return False
    
    async def get_pending_subscriptions(self, recording_id: str) -> List[Dict[str, Any]]:
        """
        Obtiene lista de suscriptores pendientes para una grabación
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            Lista de diccionarios con datos de suscriptores pendientes
        """
        try:
            subscribers_key = f"subscribers:{recording_id}"
            
            # Obtener todos los suscriptores
            all_subscribers = self.redis.hgetall(subscribers_key)
            pending_subscribers = []
            
            for user_key, data_json in all_subscribers.items():
                if user_key == "initialized":  # Skip metadata key
                    continue
                
                try:
                    data = json.loads(data_json)
                    if data.get("delivery_status") == "pending":
                        pending_subscribers.append(data)
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON data for {user_key} in {recording_id}: {e}")
                    continue
            
            logger.debug(f"🔍 Found {len(pending_subscribers)} pending subscribers for {recording_id}")
            return pending_subscribers
            
        except Exception as e:
            logger.error(f"❌ Error getting pending subscriptions for {recording_id}: {e}")
            return []
    
    async def mark_delivered(self, recording_id: str, user_id: int) -> bool:
        """
        Marca entrega como confirmada para un suscriptor
        
        Para grabaciones fragmentadas, los suscriptores permanecen "pending" hasta que
        todos los fragmentos estén completados para evitar perder suscriptores en fragmentos posteriores.
        
        Args:
            recording_id: ID de la grabación
            user_id: ID del usuario que recibió su copia
            
        Returns:
            True si se marcó exitosamente, False si falló
        """
        try:
            subscribers_key = f"subscribers:{recording_id}"
            user_key = f"user_{user_id}"
            
            # Obtener datos actuales del suscriptor
            current_data = self.redis.hget(subscribers_key, user_key)
            if not current_data:
                logger.warning(f"⚠️ Subscriber {user_id} not found for recording {recording_id}")
                return False
            
            # Verificar si es una grabación fragmentada
            is_fragmented = await self.is_fragmented_recording(recording_id)
            all_fragments_completed = await self.are_all_fragments_completed(recording_id)
            
            # Actualizar estado de entrega
            data = json.loads(current_data)
            
            if is_fragmented and not all_fragments_completed:
                # Para grabaciones fragmentadas, mantener como "pending" hasta que todos los fragmentos estén listos
                data["delivery_status"] = "pending"  # Mantener pending
                data["fragments_received"] = data.get("fragments_received", 0) + 1
                data["last_fragment_delivered_at"] = str(int(time.time()))
                logger.info(f"📹 Fragment delivered to subscriber {user_id} in fragmented recording {recording_id} (keeping PENDING until all fragments complete)")
            else:
                # Para grabaciones normales O fragmentadas con todos los fragmentos completados
                data["delivery_status"] = "delivered"
                data["delivered_at"] = str(int(time.time()))
                if is_fragmented:
                    logger.info(f"✅ ALL fragments delivered to subscriber {user_id} in recording {recording_id} - marked as DELIVERED")
                else:
                    logger.info(f"✅ Marked delivery confirmed for user {user_id} in recording {recording_id}")
            
            # Guardar cambios
            self.redis.hset(subscribers_key, user_key, json.dumps(data))
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error marking delivery for user {user_id} in {recording_id}: {e}")
            return False
    
    async def mark_primary_delivered(self, recording_id: str) -> bool:
        """
        Marca que el usuario primario recibió su copia
        
        Para grabaciones fragmentadas, el usuario primario permanece "pending" hasta que
        todos los fragmentos estén completados para evitar cleanup prematuro.
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            True si se marcó exitosamente
        """
        try:
            primary_key = f"primary_delivery:{recording_id}"
            
            # Verificar si es una grabación fragmentada
            is_fragmented = await self.is_fragmented_recording(recording_id)
            all_fragments_completed = await self.are_all_fragments_completed(recording_id)
            
            # Actualizar estado según fragmentación
            with self.redis.pipeline() as pipe:
                pipe.multi()
                pipe.hset(primary_key, "upload_status", "completed")
                
                if is_fragmented and not all_fragments_completed:
                    # Para grabaciones fragmentadas, mantener como "pending" hasta que todos los fragmentos estén listos
                    pipe.hset(primary_key, "delivery_status", "pending")  # Mantener pending
                    pipe.hset(primary_key, "last_fragment_delivered_at", str(int(time.time())))
                    logger.info(f"📹 Fragment delivered to primary user for fragmented recording {recording_id} (keeping PENDING until all fragments complete)")
                else:
                    # Para grabaciones normales O fragmentadas con todos los fragmentos completados
                    pipe.hset(primary_key, "delivery_status", "delivered")
                    pipe.hset(primary_key, "delivered_at", str(int(time.time())))
                    if is_fragmented:
                        logger.info(f"✅ ALL fragments delivered to primary user for recording {recording_id} - marked as DELIVERED")
                    else:
                        logger.info(f"✅ Marked primary user delivery confirmed for {recording_id}")
                
                pipe.expire(primary_key, self.recording_ttl)
                pipe.execute()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error marking primary delivery for {recording_id}: {e}")
            return False
    
    async def increment_primary_upload_attempts(self, recording_id: str) -> int:
        """
        Incrementa el contador de intentos de upload del usuario primario
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            Número actual de intentos después del incremento
        """
        try:
            primary_key = f"primary_delivery:{recording_id}"
            
            # Incrementar contador atómicamente
            current_attempts = self.redis.hincrby(primary_key, "upload_attempts", 1)
            self.redis.hset(primary_key, "last_attempt_at", str(int(time.time())))
            
            logger.info(f"🔄 Primary upload attempt {current_attempts} for {recording_id}")
            return current_attempts
            
        except Exception as e:
            logger.error(f"❌ Error incrementing primary upload attempts for {recording_id}: {e}")
            return 0
    
    async def mark_primary_upload_failed(self, recording_id: str) -> bool:
        """
        Marca que el upload del usuario primario ha fallado definitivamente
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            True si se marcó exitosamente
        """
        try:
            primary_key = f"primary_delivery:{recording_id}"
            
            # Marcar como fallido definitivamente
            with self.redis.pipeline() as pipe:
                pipe.multi()
                pipe.hset(primary_key, "upload_status", "failed")
                pipe.hset(primary_key, "delivery_status", "failed")
                pipe.hset(primary_key, "failed_at", str(int(time.time())))
                pipe.expire(primary_key, self.recording_ttl)
                pipe.execute()
            
            logger.warning(f"❌ Marked primary upload as FAILED for {recording_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error marking primary upload as failed for {recording_id}: {e}")
            return False
    
    async def _check_primary_user_delivered(self, recording_id: str) -> bool:
        """
        Verifica si el usuario primario ya recibió su copia
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            True si el usuario primario ya recibió su copia
        """
        try:
            primary_key = f"primary_delivery:{recording_id}"
            primary_data = self.redis.hgetall(primary_key)
            
            if not primary_data:
                return False
            
            delivery_status = primary_data.get("delivery_status", "pending")
            return delivery_status == "delivered"
            
        except Exception as e:
            logger.error(f"❌ Error checking primary delivery status for {recording_id}: {e}")
            return False
    
    async def mark_recording_processing(self, recording_id: str) -> bool:
        """
        Marca que la grabación ha terminado de grabar y está siendo procesada
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            True si se marcó exitosamente
        """
        try:
            username = recording_id.split('_')[0]
            session_keys = self.redis.keys(f"recording:{username}:*")
            
            for session_key in session_keys:
                try:
                    session_data = self.redis.hgetall(session_key)
                    if session_data.get("recording_id") == recording_id:
                        self.redis.hset(session_key, "processing_status", "processing")
                        self.redis.hset(session_key, "processing_started_at", str(int(time.time())))
                        logger.info(f"🔄 Marked recording as PROCESSING: {recording_id}")
                        return True
                except Exception:
                    continue
            
            logger.warning(f"⚠️ Session not found for processing mark: {recording_id}")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error marking recording as processing for {recording_id}: {e}")
            return False
    
    async def mark_recording_processed(self, recording_id: str) -> bool:
        """
        Marca que la grabación ha sido procesada exitosamente (MP4 listo)
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            True si se marcó exitosamente
        """
        try:
            username = recording_id.split('_')[0]
            session_keys = self.redis.keys(f"recording:{username}:*")
            
            for session_key in session_keys:
                try:
                    session_data = self.redis.hgetall(session_key)
                    if session_data.get("recording_id") == recording_id:
                        self.redis.hset(session_key, "processing_status", "processed")
                        self.redis.hset(session_key, "processed_at", str(int(time.time())))
                        logger.info(f"✅ Marked recording as PROCESSED: {recording_id}")
                        return True
                except Exception:
                    continue
            
            logger.warning(f"⚠️ Session not found for processed mark: {recording_id}")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error marking recording as processed for {recording_id}: {e}")
            return False
    
    async def mark_recording_failed(self, recording_id: str, reason: str = "unknown") -> bool:
        """
        Marca que la grabación ha fallado en el procesamiento
        
        Args:
            recording_id: ID de la grabación
            reason: Razón del fallo
            
        Returns:
            True si se marcó exitosamente
        """
        try:
            username = recording_id.split('_')[0]
            session_keys = self.redis.keys(f"recording:{username}:*")
            
            for session_key in session_keys:
                try:
                    session_data = self.redis.hgetall(session_key)
                    if session_data.get("recording_id") == recording_id:
                        self.redis.hset(session_key, "processing_status", "failed")
                        self.redis.hset(session_key, "failed_at", str(int(time.time())))
                        self.redis.hset(session_key, "failure_reason", reason)
                        logger.warning(f"❌ Marked recording as FAILED: {recording_id} (reason: {reason})")
                        return True
                except Exception:
                    continue
            
            logger.warning(f"⚠️ Session not found for failed mark: {recording_id}")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error marking recording as failed for {recording_id}: {e}")
            return False
    
    async def mark_recording_fragmented(self, recording_id: str, fragment_number: int) -> bool:
        """
        Marca que la grabación ha sido fragmentada y actualiza el número de fragmento actual
        
        Args:
            recording_id: ID de la grabación
            fragment_number: Número del fragmento actual
            
        Returns:
            True si se marcó exitosamente
        """
        try:
            username = recording_id.split('_')[0]
            session_keys = self.redis.keys(f"recording:{username}:*")
            
            for session_key in session_keys:
                try:
                    session_data = self.redis.hgetall(session_key)
                    if session_data.get("recording_id") == recording_id:
                        self.redis.hset(session_key, "is_fragmented", "true")
                        self.redis.hset(session_key, "current_fragment", str(fragment_number))
                        self.redis.hset(session_key, "fragment_count", str(fragment_number))
                        self.redis.hset(session_key, "last_activity", str(int(time.time())))
                        logger.info(f"📹 Marked recording as FRAGMENTED: {recording_id} (fragment {fragment_number})")
                        return True
                except Exception:
                    continue
            
            logger.warning(f"⚠️ Session not found for fragmentation mark: {recording_id}")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error marking recording as fragmented for {recording_id}: {e}")
            return False
    
    async def mark_all_fragments_completed(self, recording_id: str) -> bool:
        """
        Marca que todos los fragmentos de la grabación han sido completados
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            True si se marcó exitosamente
        """
        try:
            username = recording_id.split('_')[0]
            session_keys = self.redis.keys(f"recording:{username}:*")
            
            for session_key in session_keys:
                try:
                    session_data = self.redis.hgetall(session_key)
                    if session_data.get("recording_id") == recording_id:
                        self.redis.hset(session_key, "all_fragments_completed", "true")
                        self.redis.hset(session_key, "fragments_completed_at", str(int(time.time())))
                        logger.info(f"✅ Marked ALL fragments completed for: {recording_id}")
                        return True
                except Exception:
                    continue
            
            logger.warning(f"⚠️ Session not found for fragments completion mark: {recording_id}")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error marking all fragments completed for {recording_id}: {e}")
            return False
    
    async def is_fragmented_recording(self, recording_id: str) -> bool:
        """
        Verifica si una grabación es fragmentada
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            True si la grabación es fragmentada
        """
        try:
            username = recording_id.split('_')[0]
            session_keys = self.redis.keys(f"recording:{username}:*")
            
            for session_key in session_keys:
                try:
                    session_data = self.redis.hgetall(session_key)
                    if session_data.get("recording_id") == recording_id:
                        is_fragmented = session_data.get("is_fragmented", "false")
                        return is_fragmented.lower() == "true"
                except Exception:
                    continue
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking if recording is fragmented {recording_id}: {e}")
            return False
    
    async def are_all_fragments_completed(self, recording_id: str) -> bool:
        """
        Verifica si todos los fragmentos de una grabación han sido completados
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            True si todos los fragmentos están completados
        """
        try:
            username = recording_id.split('_')[0]
            session_keys = self.redis.keys(f"recording:{username}:*")
            
            for session_key in session_keys:
                try:
                    session_data = self.redis.hgetall(session_key)
                    if session_data.get("recording_id") == recording_id:
                        all_fragments_completed = session_data.get("all_fragments_completed", "false")
                        return all_fragments_completed.lower() == "true"
                except Exception:
                    continue
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking if all fragments completed {recording_id}: {e}")
            return False
    
    async def finalize_fragmented_recording_deliveries(self, recording_id: str) -> bool:
        """
        Finaliza las entregas de una grabación fragmentada marcando todos los suscriptores 
        y el usuario primario como "delivered" cuando todos los fragmentos están completados
        
        Args:
            recording_id: ID de la grabación fragmentada
            
        Returns:
            True si se finalizó exitosamente
        """
        try:
            # Verificar que todos los fragmentos estén completados
            if not await self.are_all_fragments_completed(recording_id):
                logger.debug(f"🔍 Not all fragments completed for {recording_id}, skipping finalization")
                return False
            
            subscribers_key = f"subscribers:{recording_id}"
            primary_key = f"primary_delivery:{recording_id}"
            
            # Operación atómica para finalizar todas las entregas
            with self.redis.pipeline() as pipe:
                pipe.multi()
                
                # 1. Finalizar entregas de suscriptores
                all_subscribers = self.redis.hgetall(subscribers_key)
                finalized_subscribers = 0
                
                for user_key, data_json in all_subscribers.items():
                    if user_key == "initialized":
                        continue
                    try:
                        data = json.loads(data_json)
                        if data.get("delivery_status") == "pending":
                            data["delivery_status"] = "delivered"
                            data["delivered_at"] = str(int(time.time()))
                            data["finalized_fragments"] = "true"
                            pipe.hset(subscribers_key, user_key, json.dumps(data))
                            finalized_subscribers += 1
                    except json.JSONDecodeError:
                        continue
                
                # 2. Finalizar entrega del usuario primario
                primary_data = self.redis.hgetall(primary_key)
                if primary_data and primary_data.get("delivery_status") == "pending":
                    pipe.hset(primary_key, "delivery_status", "delivered")
                    pipe.hset(primary_key, "delivered_at", str(int(time.time())))
                    pipe.hset(primary_key, "finalized_fragments", "true")
                
                pipe.execute()
            
            logger.info(f"✅ Finalized fragmented recording deliveries for {recording_id} (primary + {finalized_subscribers} subscribers)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error finalizing fragmented recording deliveries for {recording_id}: {e}")
            return False
    
    async def get_recording_processing_status(self, recording_id: str) -> str:
        """
        Obtiene el estado de procesamiento de una grabación
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            Estado de procesamiento: 'recording', 'processing', 'processed', 'failed', 'not_found'
        """
        try:
            username = recording_id.split('_')[0]
            session_keys = self.redis.keys(f"recording:{username}:*")
            
            for session_key in session_keys:
                try:
                    session_data = self.redis.hgetall(session_key)
                    if session_data.get("recording_id") == recording_id:
                        return session_data.get("processing_status", "recording")
                except Exception:
                    continue
            
            return "not_found"
            
        except Exception as e:
            logger.error(f"❌ Error getting processing status for {recording_id}: {e}")
            return "not_found"
    
    async def is_recording_ready_for_subscribers(self, recording_id: str) -> bool:
        """
        Verifica si la grabación acepta nuevos suscriptores usando pipeline Redis optimizado
        
        OPTIMIZACIÓN: Una sola consulta pipeline Redis en lugar de 6-8 consultas secuenciales
        para evitar timeouts y mejorar performance drasticamente.
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            True si la grabación acepta nuevos suscriptores
        """
        try:
            username = recording_id.split('_')[0]
            current_time = int(time.time())
            timeout_seconds = config.recording_timeout_hours * 3600
            
            # PIPELINE REDIS: Obtener todos los datos necesarios en UNA SOLA operación
            with self.redis.pipeline() as pipe:
                # 1. Verificar si está en pending_deliveries
                pipe.zscore("pending_deliveries", recording_id)
                
                # 2. Obtener primary delivery status
                pipe.hgetall(f"primary_delivery:{recording_id}")
                
                # 3. Buscar datos de sesión para este recording_id
                pipe.keys(f"recording:{username}:*")
                
                pipeline_results = pipe.execute()
            
            # Procesar resultados del pipeline
            pending_score = pipeline_results[0]
            primary_data = pipeline_results[1]
            session_keys = pipeline_results[2]
            
            # VERIFICACIÓN 1: Debe estar en pending_deliveries
            if not pending_score:
                logger.debug(f"📹 Recording {recording_id} not in pending_deliveries - not accepting subscribers")
                return False
            
            # VERIFICACIÓN 2: Obtener datos de sesión con segundo pipeline (si es necesario)
            session_data = {}
            if session_keys:
                with self.redis.pipeline() as pipe:
                    for session_key in session_keys:
                        pipe.hgetall(session_key)
                    session_results = pipe.execute()
                
                # Encontrar la sesión que corresponde a nuestro recording_id
                for i, session_key in enumerate(session_keys):
                    data = session_results[i]
                    if data and data.get("recording_id") == recording_id:
                        session_data = data
                        break
            
            # PROCESAMIENTO LOCAL (sin más consultas Redis)
            
            # Verificar estado de procesamiento
            processing_status = session_data.get("processing_status", "recording")
            if processing_status in ["failed", "not_found"]:
                logger.debug(f"📹 Recording {recording_id} not ready: processing_status={processing_status}")
                return False
            
            # Verificar actividad reciente
            if session_data:
                last_activity = int(session_data.get("last_activity", current_time))
                if current_time - last_activity > timeout_seconds:
                    logger.debug(f"📹 Recording {recording_id} inactive for {(current_time - last_activity)/3600:.1f}h - not accepting subscribers")
                    return False
            
            # Verificar si está completamente terminada
            primary_delivered = primary_data.get("delivery_status") == "delivered" if primary_data else False
            is_fragmented = session_data.get("is_fragmented", "false").lower() == "true"
            all_fragments_completed = session_data.get("all_fragments_completed", "false").lower() == "true"
            
            if is_fragmented:
                # Para grabaciones fragmentadas: Solo rechazar si todos los fragmentos están completados Y entregados
                if all_fragments_completed and primary_delivered:
                    logger.debug(f"📹 Fragmented recording {recording_id} fully completed - not accepting subscribers")
                    return False
                else:
                    logger.debug(f"📹 Fragmented recording {recording_id} still active (completed: {all_fragments_completed}, delivered: {primary_delivered}) - accepting subscribers")
            else:
                # Para grabaciones normales: Solo rechazar si ya fue entregada
                if primary_delivered:
                    logger.debug(f"📹 Non-fragmented recording {recording_id} already delivered - not accepting subscribers")
                    return False
            
            # Si llegamos aquí: Grabación está activa, no completada, y con actividad reciente
            logger.debug(f"✅ Recording {recording_id} ready for subscribers (status: {processing_status}, fragmented: {is_fragmented}, completed: {all_fragments_completed}, delivered: {primary_delivered})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error checking if recording ready for subscribers {recording_id}: {e}")
            # En caso de error, ser permisivo y asumir que acepta suscriptores
            # Es mejor permitir una suscripción que crear grabaciones duplicadas
            return True
    
    async def is_recording_uploaded(self, recording_id: str) -> bool:
        """
        Verifica si la grabación ya fue cargada (para filtro de grabaciones activas)
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            True si la grabación ya fue cargada al usuario primario
        """
        try:
            primary_key = f"primary_delivery:{recording_id}"
            primary_data = self.redis.hgetall(primary_key)
            
            if not primary_data:
                return False
            
            upload_status = primary_data.get("upload_status", "pending")
            return upload_status in ["completed", "failed"]
            
        except Exception as e:
            logger.error(f"❌ Error checking upload status for {recording_id}: {e}")
            return False
    
    async def cleanup_completed_recording(self, recording_id: str) -> bool:
        """
        Limpia COMPLETAMENTE una grabación solo si:
        1. Usuario primario recibió su copia
        2. TODOS los suscriptores recibieron sus copias
        
        Args:
            recording_id: ID de la grabación a limpiar
            
        Returns:
            True si se limpió completamente, False si aún hay entregas pendientes
        """
        try:
            subscribers_key = f"subscribers:{recording_id}"
            
            # 1. Verificar que el usuario primario recibió su copia
            primary_delivered = await self._check_primary_user_delivered(recording_id)
            if not primary_delivered:
                logger.debug(f"🔍 Primary user delivery not confirmed yet for {recording_id}")
                return False
            
            # 2. Contar suscriptores pendientes
            all_subscribers = self.redis.hgetall(subscribers_key)
            pending_count = 0
            delivered_count = 0
            
            for user_key, data_json in all_subscribers.items():
                if user_key == "initialized":
                    continue
                try:
                    data = json.loads(data_json)
                    status = data.get("delivery_status", "pending")
                    if status == "pending":
                        pending_count += 1
                    elif status == "delivered":
                        delivered_count += 1
                except json.JSONDecodeError:
                    continue
            
            # 3. Solo proceder con cleanup si TODAS las entregas están confirmadas Y no hay fragmentos activos
            if pending_count == 0:
                # VERIFICACIÓN ADICIONAL: Replicar lógica de memoria para grabaciones fragmentadas
                username = recording_id.split('_')[0]
                has_active_fragments = self._has_active_fragments(username)
                
                if has_active_fragments:
                    logger.info(f"⏸️ Cleanup skipped for {recording_id} - fragmented recording still has active fragments")
                    return False
                
                logger.info(f"🧹 All deliveries confirmed for {recording_id} (primary + {delivered_count} subscribers), performing COMPLETE cleanup")
                
                # OPERACIÓN ATÓMICA DE CLEANUP COMPLETO
                with self.redis.pipeline() as pipe:
                    pipe.multi()
                    
                    # A. Remover de cola de entregas pendientes
                    pipe.zrem("pending_deliveries", recording_id)
                    
                    # B. Limpiar suscripciones de cada usuario
                    for user_key, data_json in all_subscribers.items():
                        if user_key == "initialized":
                            continue
                        try:
                            data = json.loads(data_json)
                            user_id = data.get("user_id")
                            if user_id:
                                pipe.srem(f"user_subs:{user_id}", recording_id)
                        except json.JSONDecodeError:
                            continue
                    
                    # C. Eliminar estructura completa de suscriptores
                    pipe.delete(subscribers_key)
                    
                    # D. Eliminar estado de entrega primaria
                    pipe.delete(f"primary_delivery:{recording_id}")
                    
                    # E. Limpiar sesión de grabación
                    username = recording_id.split('_')[0]
                    session_keys = self.redis.keys(f"recording:{username}:*")
                    for session_key in session_keys:
                        try:
                            session_data = self.redis.hgetall(session_key)
                            if session_data.get("recording_id") == recording_id:
                                pipe.delete(session_key)
                                break
                        except Exception:
                            continue
                    
                    # F. Limpiar claves huérfanas de grabaciones fallback del mismo usuario
                    orphan_keys = self.redis.keys(f"primary_delivery:{username}_*_fallback")
                    for orphan_key in orphan_keys:
                        pipe.delete(orphan_key)
                    
                    # G. Limpiar cualquier metadata adicional
                    pipe.delete(f"metadata:{recording_id}")
                    
                    pipe.execute()
                
                logger.info(f"✅ COMPLETE cleanup finished for {recording_id}")
                return True
            
            else:
                logger.debug(f"🔍 Still {pending_count} pending deliveries for {recording_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error during cleanup for {recording_id}: {e}")
            return False
    
    async def get_user_subscriptions(self, user_id: int) -> List[str]:
        """
        Obtiene lista de recording_ids a los que está suscrito un usuario
        
        Args:
            user_id: ID del usuario
            
        Returns:
            Lista de recording_ids activos para el usuario
        """
        try:
            user_subs_key = f"user_subs:{user_id}"
            return list(self.redis.smembers(user_subs_key))
            
        except Exception as e:
            logger.error(f"❌ Error getting user subscriptions for {user_id}: {e}")
            return []
    
    # Método recover_pending_recordings eliminado - el recovery de grabaciones 
    # causaba grabaciones fantasma y no aportaba valor real
    
    async def get_recording_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas del sistema de grabaciones
        
        Returns:
            Diccionario con estadísticas actuales
        """
        try:
            stats = {
                "pending_recordings": self.redis.zcard("pending_deliveries"),
                "total_subscribers": 0,
                "pending_deliveries": 0,
                "completed_deliveries": 0
            }
            
            # Contar suscriptores y entregas
            pending_recordings = self.redis.zrange("pending_deliveries", 0, -1)
            for recording_id in pending_recordings:
                subscribers_key = f"subscribers:{recording_id}"
                all_subs = self.redis.hgetall(subscribers_key)
                
                for user_key, data_json in all_subs.items():
                    if user_key == "initialized":
                        continue
                    try:
                        data = json.loads(data_json)
                        stats["total_subscribers"] += 1
                        if data.get("delivery_status") == "pending":
                            stats["pending_deliveries"] += 1
                        else:
                            stats["completed_deliveries"] += 1
                    except json.JSONDecodeError:
                        continue
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error getting recording stats: {e}")
            return {}
    
    async def save_upload_result(self, recording_id: str, message_ids: Dict[str, Any], primary_chat_id: int) -> bool:
        """
        Guarda los message_ids del upload principal para forwarding posterior
        
        Args:
            recording_id: ID de la grabación
            message_ids: Diccionario con IDs de mensajes enviados
            primary_chat_id: Chat ID del usuario primario
            
        Returns:
            True si se guardó exitosamente
        """
        try:
            upload_key = f"upload_result:{recording_id}"
            
            # Serializar message_ids manteniendo el tipo original
            if isinstance(message_ids, (list, tuple)):
                message_ids_str = json.dumps(list(message_ids))
                message_ids_type = "list"
            elif isinstance(message_ids, int):
                message_ids_str = str(message_ids)
                message_ids_type = "int"
            elif isinstance(message_ids, dict):
                message_ids_str = json.dumps(message_ids)
                message_ids_type = "dict"
            else:
                message_ids_str = json.dumps(message_ids)
                message_ids_type = "other"
            
            upload_data = {
                "recording_id": recording_id,
                "message_ids": message_ids_str,
                "message_ids_type": message_ids_type,
                "primary_chat_id": str(primary_chat_id),
                "upload_status": "completed",
                "uploaded_at": str(int(time.time())),
                "forwarding_status": "pending"
            }
            
            # Operación atómica para guardar resultado de upload
            with self.redis.pipeline() as pipe:
                pipe.multi()
                
                # 1. Guardar datos de upload
                pipe.hset(upload_key, mapping=upload_data)
                
                # 2. Añadir a cola de forwarding pendiente
                pipe.zadd("pending_forwarding", {recording_id: time.time()})
                
                # 3. Configurar TTL
                pipe.expire(upload_key, self.recording_ttl)
                pipe.expire("pending_forwarding", self.recording_ttl)
                
                pipe.execute()
            
            logger.info(f"✅ Saved upload result for {recording_id} with message_ids to Redis")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving upload result for {recording_id}: {e}")
            return False
    
    async def get_upload_result(self, recording_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene los message_ids y datos de upload desde Redis
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            Diccionario con datos de upload o None si no existe
        """
        try:
            upload_key = f"upload_result:{recording_id}"
            upload_data = self.redis.hgetall(upload_key)
            
            if not upload_data:
                logger.warning(f"⚠️ No upload result found for {recording_id}")
                return None
            
            # Deserializar message_ids manteniendo el tipo original
            try:
                message_ids_str = upload_data.get("message_ids", "")
                message_ids_type = upload_data.get("message_ids_type", "other")
                
                if message_ids_type == "list":
                    message_ids = json.loads(message_ids_str)
                elif message_ids_type == "int":
                    message_ids = int(message_ids_str)
                elif message_ids_type == "dict":
                    message_ids = json.loads(message_ids_str)
                else:
                    message_ids = json.loads(message_ids_str)
                
                upload_data["message_ids"] = message_ids
                logger.debug(f"🔍 Deserialized message_ids for {recording_id}: {message_ids} (type: {type(message_ids)})")
                
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"❌ Invalid message_ids data for {recording_id}: {e}")
                return None
            
            logger.debug(f"🔍 Retrieved upload result for {recording_id}")
            return upload_data
            
        except Exception as e:
            logger.error(f"❌ Error getting upload result for {recording_id}: {e}")
            return None
    
    async def mark_upload_completed(self, recording_id: str) -> bool:
        """
        Marca que el upload principal ha completado exitosamente
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            True si se marcó exitosamente
        """
        try:
            upload_key = f"upload_result:{recording_id}"
            
            # Verificar que existe el upload result
            if not self.redis.exists(upload_key):
                logger.warning(f"⚠️ Upload result not found for {recording_id}")
                return False
            
            # Actualizar estado
            self.redis.hset(upload_key, "upload_status", "completed")
            self.redis.hset(upload_key, "completed_at", str(int(time.time())))
            
            logger.info(f"✅ Marked upload completed for {recording_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error marking upload completed for {recording_id}: {e}")
            return False
    
    async def mark_forwarding_completed(self, recording_id: str) -> bool:
        """
        Marca que el forwarding a suscriptores ha completado
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            True si se marcó exitosamente
        """
        try:
            upload_key = f"upload_result:{recording_id}"
            
            # Operación atómica para completar forwarding
            with self.redis.pipeline() as pipe:
                pipe.multi()
                
                # 1. Actualizar estado de forwarding
                pipe.hset(upload_key, "forwarding_status", "completed")
                pipe.hset(upload_key, "forwarding_completed_at", str(int(time.time())))
                
                # 2. Remover de cola de forwarding pendiente
                pipe.zrem("pending_forwarding", recording_id)
                
                pipe.execute()
            
            logger.info(f"✅ Marked forwarding completed for {recording_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error marking forwarding completed for {recording_id}: {e}")
            return False
    
    async def get_pending_forwarding(self) -> List[str]:
        """
        Obtiene lista de grabaciones con forwarding pendiente
        
        Returns:
            Lista de recording_ids con forwarding pendiente
        """
        try:
            pending_recordings = self.redis.zrange("pending_forwarding", 0, -1)
            
            if pending_recordings:
                logger.debug(f"🔍 Found {len(pending_recordings)} recordings with pending forwarding")
            
            return list(pending_recordings)
            
        except Exception as e:
            logger.error(f"❌ Error getting pending forwarding: {e}")
            return []
    
    async def cleanup_upload_data(self, recording_id: str) -> bool:
        """
        Limpia datos de upload después de que forwarding esté completo
        
        Args:
            recording_id: ID de la grabación
            
        Returns:
            True si se limpió exitosamente
        """
        try:
            upload_key = f"upload_result:{recording_id}"
            
            # Verificar que forwarding está completo
            upload_data = self.redis.hgetall(upload_key)
            if not upload_data:
                logger.debug(f"🔍 No upload data to clean for {recording_id}")
                return True
            
            forwarding_status = upload_data.get("forwarding_status")
            if forwarding_status != "completed":
                logger.debug(f"🔍 Forwarding not completed yet for {recording_id}, skipping cleanup")
                return False
            
            # Limpiar datos de upload
            with self.redis.pipeline() as pipe:
                pipe.multi()
                
                # 1. Eliminar datos de upload
                pipe.delete(upload_key)
                
                # 2. Asegurar que no esté en cola de forwarding
                pipe.zrem("pending_forwarding", recording_id)
                
                pipe.execute()
            
            logger.info(f"✅ Cleaned upload data for {recording_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error cleaning upload data for {recording_id}: {e}")
            return False
    
    async def cleanup_user_session_records(self, username: str, current_recording_id: str = None) -> int:
        """
        Limpia TODOS los registros recording:username:* después de upload exitoso
        Incluye la grabación actual y todas las grabaciones huérfanas del mismo usuario
        
        Args:
            username: Nombre del usuario
            current_recording_id: ID de la grabación actual (opcional, para logs)
            
        Returns:
            Número de registros limpiados
        """
        try:
            # VERIFICACIÓN CRÍTICA: No limpiar si hay fragmentos activos en progreso
            has_active_fragments = self._has_active_fragments(username)
            if has_active_fragments:
                logger.info(f"⏸️ Session cleanup skipped for {username} - fragmented recording still has active fragments")
                return 0

            session_keys = self.redis.keys(f"recording:{username}:*")
            if not session_keys:
                logger.debug(f"No session records found for user {username}")
                return 0
            
            deleted_count = 0
            with self.redis.pipeline() as pipe:
                pipe.multi()
                
                for session_key in session_keys:
                    try:
                        session_data = self.redis.hgetall(session_key)
                        stored_recording_id = session_data.get("recording_id", "unknown")
                        
                        pipe.delete(session_key)
                        deleted_count += 1
                        logger.debug(f"🗑️ Marking for deletion: {session_key} (recording_id: {stored_recording_id})")
                        
                    except Exception as e:
                        logger.warning(f"Error processing session key {session_key}: {e}")
                        continue
                
                pipe.execute()
            
            if deleted_count > 0:
                current_info = f" (current: {current_recording_id})" if current_recording_id else ""
                logger.info(f"🧹 Deleted {deleted_count} session records for user {username}{current_info}")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"❌ Error cleaning session records for user {username}: {e}")
            return 0
    
    async def cleanup_expired_recordings(self) -> int:
        """
        Limpia grabaciones que han expirado por timeout (OPCIÓN A)
        
        Returns:
            Número de grabaciones limpiadas
        """
        try:
            current_time = int(time.time())
            timeout_seconds = config.recording_timeout_hours * 3600
            cutoff_time = current_time - timeout_seconds
            
            logger.info(f"🧹 Starting cleanup of recordings older than {config.recording_timeout_hours} hours")
            
            # Obtener todas las grabaciones pendientes
            pending_recordings = self.redis.zrange("pending_deliveries", 0, -1, withscores=True)
            
            cleaned_count = 0
            expired_recordings = []
            
            # Identificar grabaciones expiradas
            for recording_id, timestamp in pending_recordings:
                if timestamp < cutoff_time:
                    expired_recordings.append(recording_id)
            
            # Limpiar cada grabación expirada
            for recording_id in expired_recordings:
                try:
                    logger.warning(f"⚠️ Cleaning expired recording: {recording_id} (age: {(current_time - cutoff_time)/3600:.1f} hours)")
                    
                    # Cleanup completo forzado
                    success = await self._force_cleanup_recording(recording_id, reason="timeout")
                    if success:
                        cleaned_count += 1
                        
                except Exception as e:
                    logger.error(f"❌ Error cleaning expired recording {recording_id}: {e}")
                    continue
            
            if cleaned_count > 0:
                logger.info(f"🧹 Cleaned {cleaned_count} expired recordings")
            else:
                logger.debug(f"🔍 No expired recordings found for cleanup")
            
            return cleaned_count
            
        except Exception as e:
            logger.error(f"❌ Error during expired recordings cleanup: {e}")
            return 0
    
    async def cleanup_failed_primary_uploads(self) -> int:
        """
        Limpia grabaciones donde el upload al usuario primario ha fallado (OPCIÓN C)
        
        Returns:
            Número de grabaciones limpiadas por upload primario fallido
        """
        try:
            max_attempts = config.forwarding_max_retry_attempts  # Reutilizar la misma config
            logger.info(f"🧹 Starting cleanup of recordings with failed primary uploads ({max_attempts}+ attempts)")
            
            # Obtener grabaciones pendientes
            pending_recordings = self.redis.zrange("pending_deliveries", 0, -1)
            
            cleaned_count = 0
            
            for recording_id in pending_recordings:
                try:
                    primary_key = f"primary_delivery:{recording_id}"
                    primary_data = self.redis.hgetall(primary_key)
                    
                    if not primary_data:
                        continue
                    
                    upload_status = primary_data.get("upload_status", "pending")
                    upload_attempts = int(primary_data.get("upload_attempts", 0))
                    
                    # Si el upload ha fallado o hay demasiados intentos
                    should_cleanup = (
                        upload_status == "failed" or 
                        (upload_status == "pending" and upload_attempts >= max_attempts)
                    )
                    
                    if should_cleanup:
                        logger.warning(f"⚠️ Cleaning recording with failed primary upload: {recording_id} (status: {upload_status}, attempts: {upload_attempts})")
                        
                        # Cleanup completo - no tiene sentido mantener si no hay video para forward
                        success = await self._force_cleanup_recording(recording_id, reason="primary_upload_failed")
                        if success:
                            cleaned_count += 1
                
                except Exception as e:
                    logger.error(f"❌ Error checking primary upload status for {recording_id}: {e}")
                    continue
            
            if cleaned_count > 0:
                logger.info(f"🧹 Cleaned {cleaned_count} recordings with failed primary uploads")
            else:
                logger.debug(f"🔍 No recordings with failed primary uploads found for cleanup")
            
            return cleaned_count
            
        except Exception as e:
            logger.error(f"❌ Error during failed primary uploads cleanup: {e}")
            return 0
    
    async def cleanup_orphaned_recordings(self) -> int:
        """
        Limpia grabaciones "huérfanas" que están marcadas como activas pero sin proceso real (OPCIÓN D)
        
        Detecta grabaciones que:
        - Están en estado 'recording' por mucho tiempo sin actividad
        - Están en estado 'processing' pero el proceso no existe
        
        Returns:
            Número de grabaciones huérfanas limpiadas
        """
        try:
            current_time = int(time.time())
            max_recording_time = config.recording_timeout_hours * 3600  # 3 horas max grabando
            max_processing_time = 30 * 60  # 30 minutos max procesando
            
            logger.info(f"🧹 Starting cleanup of orphaned recordings (max recording: {config.recording_timeout_hours}h, max processing: 30min)")
            
            # Obtener todas las grabaciones pendientes
            pending_recordings = self.redis.zrange("pending_deliveries", 0, -1)
            
            cleaned_count = 0
            
            for recording_id in pending_recordings:
                try:
                    username = recording_id.split('_')[0]
                    session_keys = self.redis.keys(f"recording:{username}:*")
                    
                    for session_key in session_keys:
                        try:
                            session_data = self.redis.hgetall(session_key)
                            if session_data.get("recording_id") != recording_id:
                                continue
                            
                            processing_status = session_data.get("processing_status", "recording")
                            start_time = int(session_data.get("start_time", current_time))
                            last_activity = int(session_data.get("last_activity", start_time))
                            
                            should_cleanup = False
                            cleanup_reason = ""
                            
                            # Caso 1: Grabando por demasiado tiempo sin actividad
                            if processing_status == "recording":
                                inactive_time = current_time - last_activity
                                total_time = current_time - start_time
                                
                                if inactive_time > max_recording_time or total_time > max_recording_time:
                                    should_cleanup = True
                                    cleanup_reason = f"recording_too_long (inactive: {inactive_time/3600:.1f}h, total: {total_time/3600:.1f}h)"
                            
                            # Caso 2: Procesando por demasiado tiempo
                            elif processing_status == "processing":
                                processing_started = int(session_data.get("processing_started_at", current_time))
                                processing_time = current_time - processing_started
                                
                                if processing_time > max_processing_time:
                                    should_cleanup = True
                                    cleanup_reason = f"processing_too_long ({processing_time/60:.1f}min)"
                            
                            # Caso 3: Estado inválido o inconsistente
                            elif processing_status in ["failed", "processed"]:
                                # Estas ya deberían haber sido limpiadas
                                should_cleanup = True
                                cleanup_reason = f"inconsistent_state ({processing_status})"
                            
                            if should_cleanup:
                                logger.warning(f"⚠️ Cleaning orphaned recording: {recording_id} (reason: {cleanup_reason})")
                                
                                # Marcar como fallida antes de limpiar
                                await self.mark_recording_failed(recording_id, cleanup_reason)
                                
                                # Cleanup completo
                                success = await self._force_cleanup_recording(recording_id, reason=f"orphaned_{cleanup_reason}")
                                if success:
                                    cleaned_count += 1
                            
                            break  # Solo procesar una sesión por recording_id
                            
                        except Exception as session_error:
                            logger.error(f"❌ Error processing session {session_key}: {session_error}")
                            continue
                
                except Exception as e:
                    logger.error(f"❌ Error checking orphaned status for {recording_id}: {e}")
                    continue
            
            if cleaned_count > 0:
                logger.info(f"🧹 Cleaned {cleaned_count} orphaned recordings")
            else:
                logger.debug(f"🔍 No orphaned recordings found for cleanup")
            
            return cleaned_count
            
        except Exception as e:
            logger.error(f"❌ Error during orphaned recordings cleanup: {e}")
            return 0
    
    async def cleanup_failed_forwarding(self) -> int:
        """
        Limpia SELECTIVAMENTE suscriptores con forwarding fallido (OPCIÓN B MEJORADA)
        No elimina toda la grabación, solo los suscriptores que han fallado
        
        Returns:
            Número de suscriptores limpiados por fallos
        """
        try:
            max_retries = config.forwarding_max_retry_attempts
            logger.info(f"🧹 Starting SELECTIVE cleanup of subscribers with {max_retries}+ forwarding failures")
            
            # Obtener grabaciones con forwarding pendiente
            pending_forwarding = await self.get_pending_forwarding()
            
            cleaned_subscribers = 0
            cleaned_recordings = 0
            
            for recording_id in pending_forwarding:
                try:
                    # Verificar retry count de suscriptores
                    subscribers_key = f"subscribers:{recording_id}"
                    all_subscribers = self.redis.hgetall(subscribers_key)
                    
                    failed_users = []
                    remaining_users = []
                    
                    for user_key, data_json in all_subscribers.items():
                        if user_key == "initialized":
                            continue
                        
                        try:
                            data = json.loads(data_json)
                            retry_count = int(data.get("retry_count", 0))
                            delivery_status = data.get("delivery_status", "pending")
                            user_id = data.get("user_id")
                            
                            if delivery_status == "pending" and retry_count >= max_retries:
                                failed_users.append((user_key, user_id, retry_count))
                            elif delivery_status == "pending":
                                remaining_users.append((user_key, user_id, retry_count))
                        
                        except (json.JSONDecodeError, ValueError):
                            continue
                    
                    # LIMPIEZA SELECTIVA: Solo remover suscriptores fallidos
                    if failed_users:
                        logger.warning(f"⚠️ Removing {len(failed_users)} failed subscribers from {recording_id} (keeping {len(remaining_users)} pending)")
                        
                        with self.redis.pipeline() as pipe:
                            pipe.multi()
                            
                            for user_key, user_id, retry_count in failed_users:
                                # Remover suscriptor fallido
                                pipe.hdel(subscribers_key, user_key)
                                
                                # Limpiar lista de suscripciones del usuario
                                if user_id:
                                    pipe.srem(f"user_subs:{user_id}", recording_id)
                                
                                logger.info(f"❌ Removed failed subscriber {user_id} from {recording_id} (retry_count: {retry_count})")
                                cleaned_subscribers += 1
                            
                            pipe.execute()
                        
                        # Si NO quedan suscriptores pendientes, cleanup completo
                        if not remaining_users:
                            logger.info(f"🧹 No remaining subscribers for {recording_id}, doing complete cleanup")
                            success = await self._force_cleanup_recording(recording_id, reason="no_remaining_subscribers")
                            if success:
                                cleaned_recordings += 1
                
                except Exception as e:
                    logger.error(f"❌ Error during selective cleanup for {recording_id}: {e}")
                    continue
            
            if cleaned_subscribers > 0 or cleaned_recordings > 0:
                logger.info(f"🧹 Selective cleanup completed: {cleaned_subscribers} failed subscribers removed, {cleaned_recordings} recordings fully cleaned")
            else:
                logger.debug(f"🔍 No failed subscribers found for cleanup")
            
            return cleaned_subscribers
            
        except Exception as e:
            logger.error(f"❌ Error during selective forwarding cleanup: {e}")
            return 0
    
    async def increment_retry_count(self, recording_id: str, user_id: int) -> bool:
        """
        Incrementa el contador de reintentos para un suscriptor
        
        Args:
            recording_id: ID de la grabación
            user_id: ID del usuario suscriptor
            
        Returns:
            True si se incrementó exitosamente
        """
        try:
            subscribers_key = f"subscribers:{recording_id}"
            user_key = f"user_{user_id}"
            
            # Obtener datos actuales
            current_data = self.redis.hget(subscribers_key, user_key)
            if not current_data:
                logger.warning(f"⚠️ Subscriber {user_id} not found for retry increment in {recording_id}")
                return False
            
            # Incrementar retry count
            data = json.loads(current_data)
            current_retries = int(data.get("retry_count", 0))
            data["retry_count"] = str(current_retries + 1)
            data["last_retry_at"] = str(int(time.time()))
            
            # Guardar cambios
            self.redis.hset(subscribers_key, user_key, json.dumps(data))
            
            logger.info(f"🔄 Incremented retry count for user {user_id} in {recording_id}: {current_retries + 1}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error incrementing retry count for user {user_id} in {recording_id}: {e}")
            return False
    
    async def _force_cleanup_recording(self, recording_id: str, reason: str = "forced") -> bool:
        """
        Fuerza la limpieza completa de una grabación sin verificar entregas
        
        Args:
            recording_id: ID de la grabación a limpiar
            reason: Razón de la limpieza forzada
            
        Returns:
            True si se limpió exitosamente
        """
        try:
            logger.warning(f"🧹 FORCE CLEANUP: {recording_id} (reason: {reason})")
            
            subscribers_key = f"subscribers:{recording_id}"
            
            # OPERACIÓN ATÓMICA DE CLEANUP FORZADO
            with self.redis.pipeline() as pipe:
                pipe.multi()
                
                # A. Remover de colas
                pipe.zrem("pending_deliveries", recording_id)
                pipe.zrem("pending_forwarding", recording_id)
                
                # B. Obtener y limpiar suscripciones de usuarios
                all_subscribers = self.redis.hgetall(subscribers_key)
                for user_key, data_json in all_subscribers.items():
                    if user_key == "initialized":
                        continue
                    try:
                        data = json.loads(data_json)
                        user_id = data.get("user_id")
                        if user_id:
                            pipe.srem(f"user_subs:{user_id}", recording_id)
                    except json.JSONDecodeError:
                        continue
                
                # C. Eliminar estructuras de datos
                pipe.delete(subscribers_key)
                pipe.delete(f"primary_delivery:{recording_id}")
                pipe.delete(f"upload_result:{recording_id}")
                
                # D. Limpiar sesiones de grabación
                username = recording_id.split('_')[0]
                session_keys = self.redis.keys(f"recording:{username}:*")
                for session_key in session_keys:
                    try:
                        session_data = self.redis.hgetall(session_key)
                        if session_data.get("recording_id") == recording_id:
                            pipe.delete(session_key)
                            break
                    except Exception:
                        continue
                
                # E. Limpiar metadatos adicionales
                pipe.delete(f"metadata:{recording_id}")
                
                pipe.execute()
            
            logger.warning(f"✅ FORCE CLEANUP completed for {recording_id} (reason: {reason})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during force cleanup for {recording_id}: {e}")
            return False
    
    async def get_cleanup_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas para el sistema de limpieza
        
        Returns:
            Diccionario con estadísticas de limpieza
        """
        try:
            current_time = int(time.time())
            timeout_seconds = config.recording_timeout_hours * 3600
            cutoff_time = current_time - timeout_seconds
            
            # Contar grabaciones por categoría
            pending_recordings = self.redis.zrange("pending_deliveries", 0, -1, withscores=True)
            pending_forwarding = self.redis.zcard("pending_forwarding")
            
            expired_count = 0
            failed_forwarding_count = 0
            
            for recording_id, timestamp in pending_recordings:
                if timestamp < cutoff_time:
                    expired_count += 1
            
            # Contar forwarding fallido
            max_retries = config.forwarding_max_retry_attempts
            pending_forwarding_list = await self.get_pending_forwarding()
            
            for recording_id in pending_forwarding_list:
                try:
                    subscribers_key = f"subscribers:{recording_id}"
                    all_subscribers = self.redis.hgetall(subscribers_key)
                    
                    for user_key, data_json in all_subscribers.items():
                        if user_key == "initialized":
                            continue
                        try:
                            data = json.loads(data_json)
                            retry_count = int(data.get("retry_count", 0))
                            delivery_status = data.get("delivery_status", "pending")
                            
                            if delivery_status == "pending" and retry_count >= max_retries:
                                failed_forwarding_count += 1
                                break
                        except (json.JSONDecodeError, ValueError):
                            continue
                except Exception:
                    continue
            
            stats = {
                "total_pending_recordings": len(pending_recordings),
                "pending_forwarding": pending_forwarding,
                "expired_recordings": expired_count,
                "failed_forwarding_recordings": failed_forwarding_count,
                "timeout_hours": config.recording_timeout_hours,
                "max_retry_attempts": max_retries,
                "cleanup_interval_minutes": config.redis_cleanup_interval_minutes
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error getting cleanup stats: {e}")
            return {}
    
    def _has_active_fragments(self, username: str) -> bool:
        """
        Verifica si una grabación tiene fragmentos activos (replicando lógica de memoria)
        
        Args:
            username: Nombre del usuario
            
        Returns:
            True si hay fragmentos activos en user_recordings (grabación fragmentada en progreso)
        """
        try:
            # Obtener WorkerRecordingService para consultar user_recordings
            from services.worker_recording_service import WorkerRecordingService
            recording_service = WorkerRecordingService.get_instance()
            
            if not recording_service:
                logger.debug(f"RecordingService not available for fragment check: {username}")
                return False
            
            # Buscar en user_recordings si hay algún fragmento activo para este username
            with recording_service._recording_lock:
                for user_id, user_recordings in recording_service.user_recordings.items():
                    if username in user_recordings:
                        fragment_number = user_recordings[username].get('fragment_number', 1)
                        logger.debug(f"🔍 Found active fragment {fragment_number} for {username}")
                        return True
            
            logger.debug(f"🔍 No active fragments found for {username}")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking active fragments for {username}: {e}")
            # En caso de error, asumir que NO hay fragmentos para permitir cleanup
            return False

    async def cleanup_orphaned_redis_records(self) -> Dict[str, int]:
        """
        Limpia registros Redis huérfanos usando pipelines optimizados
        
        Un registro se considera SEGURO de limpiar si:
        - NO existe en master_recordings (memoria local)
        - Sin actividad >3 horas
        - NO tiene processing_status = "processing|processed"
        - NO tiene upload_status = "uploading"
        
        Returns:
            Dict con estadísticas del cleanup: {'cleaned': int, 'preserved': int, 'errors': int}
        """
        from config.env_config import config
        
        stats = {'cleaned': 0, 'preserved': 0, 'errors': 0}
        timeout_seconds = config.redis_orphan_timeout_hours * 3600
        current_time = int(time.time())
        
        try:
            logger.info("🧹 Starting Redis orphaned records cleanup with pipelines...")
            
            # PIPELINE 1: Obtener todos los datos necesarios en UNA operación
            with self.redis.pipeline() as pipe:
                pipe.zrange("pending_deliveries", 0, -1, withscores=True)  # Todos los registros pending
                pipe.keys("recording:*:*")  # Todas las sesiones
                pipe.keys("primary_delivery:*")  # Todos los primary deliveries
                
                pipeline_results = pipe.execute()
            
            pending_records = dict(pipeline_results[0])  # {recording_id: score}
            session_keys = pipeline_results[1]
            primary_keys = pipeline_results[2]
            
            logger.info(f"🔍 Found {len(pending_records)} pending records, {len(session_keys)} sessions, {len(primary_keys)} primary deliveries")
            
            # Obtener master_recordings desde memoria (importar WorkerRecordingService)
            try:
                from services.worker_recording_service import WorkerRecordingService
                recording_service = WorkerRecordingService.get_instance()
                memory_recordings = {}
                
                if recording_service:
                    with recording_service._recording_lock:
                        memory_recordings = recording_service.master_recordings.copy()
                        
                logger.info(f"🧠 Found {len(memory_recordings)} active recordings in memory")
            except Exception as e:
                logger.error(f"❌ Could not access memory recordings: {e}")
                memory_recordings = {}
            
            # PIPELINE 2: Obtener detalles de sesiones y deliveries
            session_details = {}
            primary_details = {}
            
            if session_keys or primary_keys:
                with self.redis.pipeline() as pipe:
                    # Obtener datos de sesiones
                    for session_key in session_keys:
                        pipe.hgetall(session_key)
                    
                    # Obtener datos de primary deliveries  
                    for primary_key in primary_keys:
                        pipe.hgetall(primary_key)
                    
                    details_results = pipe.execute()
                
                # Procesar resultados de sesiones
                for i, session_key in enumerate(session_keys):
                    session_data = details_results[i]
                    if session_data and 'recording_id' in session_data:
                        recording_id = session_data['recording_id']
                        session_details[recording_id] = session_data
                
                # Procesar resultados de primary deliveries
                for i, primary_key in enumerate(primary_keys):
                    primary_data = details_results[len(session_keys) + i]
                    if primary_data:
                        # Extraer recording_id del key: primary_delivery:recording_id
                        recording_id = primary_key.split(':', 1)[1]
                        primary_details[recording_id] = primary_data
            
            # ANÁLISIS Y DETERMINACIÓN DE REGISTROS SEGUROS PARA CLEANUP
            records_to_clean = []
            
            for recording_id, redis_score in pending_records.items():
                try:
                    username = recording_id.split('_')[0]
                    session_data = session_details.get(recording_id, {})
                    primary_data = primary_details.get(recording_id, {})
                    
                    # VERIFICACIÓN 1: ¿Existe en memoria?
                    if username in memory_recordings:
                        memory_recording_id = memory_recordings[username].get('recording_id')
                        if memory_recording_id == recording_id:
                            logger.debug(f"✅ Preserving {recording_id}: active in memory")
                            stats['preserved'] += 1
                            continue
                    
                    # VERIFICACIÓN 2: ¿Actividad reciente?
                    last_activity = int(session_data.get('last_activity', redis_score))
                    if current_time - last_activity < timeout_seconds:
                        logger.debug(f"✅ Preserving {recording_id}: recent activity ({(current_time - last_activity)/3600:.1f}h ago)")
                        stats['preserved'] += 1
                        continue
                    
                    # VERIFICACIÓN 3: ¿Estado de procesamiento crítico?
                    processing_status = session_data.get('processing_status', 'unknown')
                    if processing_status in ['processing', 'processed']:
                        logger.debug(f"✅ Preserving {recording_id}: processing_status={processing_status}")
                        stats['preserved'] += 1
                        continue
                    
                    # VERIFICACIÓN 4: ¿Upload en progreso?
                    upload_status = primary_data.get('upload_status', 'unknown')
                    if upload_status == 'uploading':
                        logger.debug(f"✅ Preserving {recording_id}: upload in progress")
                        stats['preserved'] += 1
                        continue
                    
                    # REGISTRO SEGURO PARA LIMPIAR
                    records_to_clean.append({
                        'recording_id': recording_id,
                        'username': username,
                        'inactive_hours': (current_time - last_activity) / 3600,
                        'processing_status': processing_status,
                        'upload_status': upload_status
                    })
                    
                except Exception as e:
                    logger.error(f"❌ Error analyzing {recording_id}: {e}")
                    stats['errors'] += 1
                    continue
            
            # PIPELINE 3: Cleanup atómico de registros seguros
            if records_to_clean:
                logger.info(f"🧹 Cleaning {len(records_to_clean)} orphaned records...")
                
                with self.redis.pipeline() as pipe:
                    pipe.multi()
                    
                    for record in records_to_clean:
                        recording_id = record['recording_id']
                        username = record['username']
                        
                        # Limpiar todas las estructuras relacionadas
                        pipe.zrem("pending_deliveries", recording_id)
                        pipe.delete(f"subscribers:{recording_id}")
                        pipe.delete(f"primary_delivery:{recording_id}")
                        pipe.delete(f"upload_result:{recording_id}")
                        
                        # Buscar y limpiar session keys
                        for session_key in session_keys:
                            if f"recording:{username}:" in session_key:
                                session_data = session_details.get(recording_id)
                                if session_data and session_data.get('recording_id') == recording_id:
                                    pipe.delete(session_key)
                    
                    cleanup_results = pipe.execute()
                    stats['cleaned'] = len(records_to_clean)
                
                # Log detallado de lo que se limpió
                for record in records_to_clean:
                    logger.info(f"🗑️ Cleaned orphaned record: {record['recording_id']} "
                              f"(user: {record['username']}, inactive: {record['inactive_hours']:.1f}h, "
                              f"status: {record['processing_status']}/{record['upload_status']})")
            else:
                logger.info("✅ No orphaned records found to clean")
            
            logger.info(f"🧹 Redis cleanup completed: cleaned={stats['cleaned']}, preserved={stats['preserved']}, errors={stats['errors']}")
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error during Redis orphan cleanup: {e}")
            stats['errors'] += 1
            return stats
    
    def health_check(self) -> bool:
        """
        Verifica que Redis esté funcionando correctamente
        
        Returns:
            True si Redis responde, False si hay problemas
        """
        try:
            self.redis.ping()
            return True
        except Exception as e:
            logger.error(f"❌ Redis health check failed: {e}")
            return False