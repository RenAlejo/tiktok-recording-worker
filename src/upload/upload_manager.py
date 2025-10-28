import asyncio
import os
import time
from typing import Optional, Dict
from dataclasses import dataclass
from pathlib import Path
from utils.logger_manager import logger
from upload.telegram import Telegram
from utils.enums import UploadError
from config.env_config import config

@dataclass
class UploadTask:
    file_path: str
    chat_id: int
    username: str
    retry_count: int = 0
    max_retries: int = 3
    error_type: Optional[UploadError] = None
    file_size: int = 0  # Tamaño del archivo en bytes
    fragment_number: int = 1  # Número de fragmento para videos divididos
    is_fragmented: bool = False  # Indica si es parte de una grabación fragmentada
    collage_path: Optional[str] = None  # Ruta al collage de screenshots (opcional)
    thumbnail_path: Optional[str] = None  # Ruta al thumbnail random (opcional)
    # Dual upload tracking
    main_upload_completed: bool = False  # Upload principal al usuario completado
    backup_upload_completed: bool = False  # Upload backup al grupo completado
    backup_upload_enabled: bool = False  # Si debe hacer backup upload
    # Weighted Round-Robin tracking
    enqueue_time: float = 0.0  # Timestamp cuando se añadió a la cola
    is_large_file: bool = False  # True si el archivo es considerado grande
    # Recording ID for Redis forwarding
    recording_id: Optional[str] = None  # Recording ID para forwarding a suscriptores

    def __post_init__(self):
        # Obtener el tamaño del archivo al inicializar
        if os.path.exists(self.file_path):
            self.file_size = os.path.getsize(self.file_path)
        
        # Establecer timestamp de enqueue si no se ha establecido
        if self.enqueue_time == 0.0:
            self.enqueue_time = time.time()
        
        # Determinar si es archivo grande
        threshold_bytes = config.upload_large_file_threshold_mb * 1024 * 1024
        self.is_large_file = self.file_size >= threshold_bytes
    
    def both_uploads_completed(self) -> bool:
        """Verifica si ambos uploads (principal y backup) han completado"""
        if not self.backup_upload_enabled:
            return self.main_upload_completed
        return self.main_upload_completed and self.backup_upload_completed

class UploadManager:
    _instance: Optional['UploadManager'] = None
    
    # Errores que merecen reintento
    RETRYABLE_ERRORS = {
        UploadError.CONNECTION_ERROR,
        UploadError.TIMEOUT_ERROR,
        UploadError.TELEGRAM_SERVER_ERROR,
        UploadError.FLOOD_WAIT_ERROR,
        UploadError.NETWORK_ERROR
    }
    
    # Errores fatales que no deben reintentarse
    FATAL_ERRORS = {
        UploadError.FILE_NOT_FOUND,
        UploadError.FILE_TOO_LARGE,
        UploadError.FILE_INVALID,
        UploadError.FILE_CORRUPTED
    }
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._queue: asyncio.Queue[UploadTask] = asyncio.Queue()
        # PHASE 2: Multiple concurrent workers
        self._worker_tasks: list[asyncio.Task] = []
        self._telegram: Optional[Telegram] = None
        self._backup_manager: Optional['BackupUploadManager'] = None
        self._completed_tasks: Dict[str, UploadTask] = {}  # Track completed tasks for dual upload
        self._upload_stats: Dict[str, int] = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'retried': 0,
            'fatal_errors': 0
        }
        
        # Simple FIFO scheduling (preserves enqueue_time for metrics and aging detection)
        
        # Periodic Recovery tracking
        self._recovery_worker_task: Optional[asyncio.Task] = None
        self._last_recovery_time = 0.0
        
        # Redis Cleanup tracking - DISABLED to prevent subscription issues
        # self._cleanup_worker_task: Optional[asyncio.Task] = None
        # self._last_cleanup_time = 0.0
        
        # Deduplication tracking
        self._queued_files: set[str] = set()  # Track files currently in queue
        self._processing_files: set[str] = set()  # Track files currently being processed
        
        self._initialized = True
    
    def set_telegram_instance(self, telegram: Telegram):
        """Establece la instancia de Telegram para las cargas"""
        self._telegram = telegram
    
    def set_backup_manager(self, backup_manager):
        """Establece la instancia del BackupUploadManager"""
        self._backup_manager = backup_manager
    
    def get_queue_size(self) -> int:
        """Obtiene el número actual de tareas en la cola de upload"""
        return self._queue.qsize()
    
    def get_queue_capacity_usage(self) -> float:
        """
        Obtiene el porcentaje de uso de la cola de upload
        Returns: float entre 0.0 y 1.0 (100%)
        """
        current_size = self.get_queue_size()
        max_size = config.max_upload_queue_size
        return min(current_size / max_size, 1.0) if max_size > 0 else 0.0
    
    def is_queue_at_capacity_threshold(self) -> bool:
        """
        Verifica si la cola está en el umbral de capacidad configurado
        Returns: True si la cola está >= threshold_percent
        """
        current_size = self.get_queue_size()
        max_size = config.max_upload_queue_size
        usage_percent = self.get_queue_capacity_usage() * 100
        threshold = config.upload_queue_capacity_threshold_percent
        
        # Debug logging
        logger.debug(f"📊 Upload queue capacity check: {current_size}/{max_size} tasks ({usage_percent:.1f}%), threshold: {threshold}%")
        
        is_at_threshold = usage_percent >= threshold
        if is_at_threshold:
            logger.info(f"⚠️ Upload queue at capacity threshold: {usage_percent:.1f}% >= {threshold}%")
        
        return is_at_threshold
    
    async def start(self):
        """Inicia los workers de carga y recovery system"""
        # PHASE 2: Start multiple concurrent workers
        num_workers = config.upload_concurrent_workers
        for i in range(num_workers):
            worker_task = asyncio.create_task(self._upload_worker(worker_id=i+1))
            self._worker_tasks.append(worker_task)
        
        logger.info(f"🚀 Started {num_workers} upload workers")
        
        # Iniciar recovery worker periódico
        if self._recovery_worker_task is None or self._recovery_worker_task.done():
            self._recovery_worker_task = asyncio.create_task(self._periodic_recovery_worker())
            logger.info("Upload recovery worker started")
            
            # Redis cleanup worker DISABLED to prevent subscription deletion issues
            # if self._cleanup_worker_task is None or self._cleanup_worker_task.done():
            #     self._cleanup_worker_task = asyncio.create_task(self._periodic_cleanup_worker())
            #     logger.info("Redis cleanup worker started")
            
            # Iniciar recuperación inicial de uploads pendientes
            await self._recover_pending_uploads()
    
    async def stop(self):
        """Detiene todos los workers de carga"""
        # PHASE 2: Stop multiple workers
        for i, worker_task in enumerate(self._worker_tasks):
            if worker_task and not worker_task.done():
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass
        logger.info(f"🚀 Stopped {len(self._worker_tasks)} upload workers")
            
        # Detener recovery worker
        if self._recovery_worker_task and not self._recovery_worker_task.done():
            self._recovery_worker_task.cancel()
            try:
                await self._recovery_worker_task
            except asyncio.CancelledError:
                pass
            logger.info("Upload recovery worker stopped")
        
        # Redis cleanup worker DISABLED
        # if self._cleanup_worker_task and not self._cleanup_worker_task.done():
        #     self._cleanup_worker_task.cancel()
        #     try:
        #         await self._cleanup_worker_task
        #     except asyncio.CancelledError:
        #         pass
        #     logger.info("Redis cleanup worker stopped")
            
        # Mostrar estadísticas finales
        logger.info("Upload Manager Statistics:")
        logger.info(f"Total uploads attempted: {self._upload_stats['total']}")
        logger.info(f"Successful uploads: {self._upload_stats['success']}")
        logger.info(f"Failed uploads: {self._upload_stats['failed']}")
        logger.info(f"Retried uploads: {self._upload_stats['retried']}")
        logger.info(f"Fatal errors: {self._upload_stats['fatal_errors']}")
    
    async def add_upload_task(self, file_path: str, chat_id: int, username: str, fragment_number: int = 1, is_fragmented: bool = False, collage_path: Optional[str] = None, thumbnail_path: Optional[str] = None):
        """Añade una nueva tarea de carga a la cola FIFO (first-in-first-out)"""
        if not self._telegram:
            raise RuntimeError("Telegram instance not set")
            
        # Verificar que el archivo existe antes de añadirlo a la cola
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            self._upload_stats['fatal_errors'] += 1
            return
        
        # DEDUPLICATION: Verificar si el archivo ya está en cola o siendo procesado
        if file_path in self._queued_files or file_path in self._processing_files:
            logger.warning(f"⚠️ File already queued or being processed, skipping duplicate: {os.path.basename(file_path)}")
            return
            
        # Determinar si debe hacer backup forwarding (método simplificado)
        backup_enabled = (config.enable_message_forwarding and config.backup_group_chat_id) or (config.enable_backup_forwarding and config.backup_group_chat_id)
        
        # Obtener recording_id de master_recordings antes de que se limpie
        recording_id = None
        try:
            recording_service = self._get_recording_service()
            if recording_service:
                recording_data = recording_service.master_recordings.get(username)
                if recording_data:
                    recording_id = recording_data.get('recording_id')
        except Exception as e:
            logger.warning(f"Could not obtain recording_id for {username}: {e}")
        
        task = UploadTask(
            file_path=file_path, 
            chat_id=chat_id, 
            username=username, 
            fragment_number=fragment_number, 
            is_fragmented=is_fragmented, 
            collage_path=collage_path, 
            thumbnail_path=thumbnail_path,
            backup_upload_enabled=backup_enabled,
            recording_id=recording_id
        )
        
        # Crear un ID único para el task para tracking dual
        task_id = f"{username}_{fragment_number}_{file_path}"
        self._completed_tasks[task_id] = task
        
        # DEDUPLICATION: Marcar archivo como encolado
        self._queued_files.add(file_path)
        
        # FIFO: First-in-first-out processing order
        await self._queue.put(task)
        self._upload_stats['total'] += 1
        logger.info(f"Added upload task for {username} (fragment {fragment_number}) to queue (size: {task.file_size/1024/1024:.2f}MB)")
        
        # Si backup está habilitado, el backup se manejará DESPUÉS del upload principal
        # para usar message forwarding si está disponible
        if backup_enabled:
            logger.info(f"Message forwarding enabled for {username}, will forward after main upload")
    
    def _should_retry(self, error_type: UploadError, retry_count: int) -> bool:
        """Determina si una tarea debe reintentarse basado en el tipo de error"""
        if error_type in self.FATAL_ERRORS:
            return False
        if error_type in self.RETRYABLE_ERRORS and retry_count < 3:
            return True
        return False
    
    async def _handle_backup_upload(self, task: UploadTask, upload_result, task_id: str):
        """Maneja el backup forwarding usando el bot principal"""
        if not task.backup_upload_enabled:
            return
            
        try:
            # Si upload_result contiene message_id(s) y forwarding está habilitado, usar forwarding con bot principal
            if upload_result and (config.enable_message_forwarding or config.enable_backup_forwarding) and config.backup_group_chat_id:
                
                logger.info(f"Forwarding message to backup group for {task.username}")
                
                # Usar el mismo telegram instance (bot principal) para el forwarding
                backup_chat_id = int(config.backup_group_chat_id)
                forward_success = await self._telegram.forward_video_to_backup(
                    message_ids=upload_result,  # Puede ser int o lista
                    source_chat_id=task.chat_id,
                    backup_chat_id=backup_chat_id,
                    username=task.username
                )
                
                if forward_success:
                    logger.info(f"Successfully forwarded video message for {task.username} to backup group")
                else:
                    logger.warning(f"Failed to forward video message for {task.username} to backup group")
                
                # Marcar backup como completado independientemente del resultado para permitir cleanup
                if task_id in self._completed_tasks:
                    self._completed_tasks[task_id].backup_upload_completed = True
                    self._check_and_cleanup_file(task_id)
            else:
                logger.debug(f"Message forwarding not available for {task.username} (upload_result: {upload_result}, forwarding_enabled: {config.enable_message_forwarding or config.enable_backup_forwarding}, backup_chat_id: {config.backup_group_chat_id})")
                # Sin forwarding, marcar como completado para permitir cleanup
                if task_id in self._completed_tasks:
                    self._completed_tasks[task_id].backup_upload_completed = True
                    self._check_and_cleanup_file(task_id)
                
        except Exception as e:
            logger.error(f"Error handling backup forwarding for {task.username}: {e}")
            # Marcar backup como fallido para permitir cleanup
            if task_id in self._completed_tasks:
                self._completed_tasks[task_id].backup_upload_completed = True
                self._check_and_cleanup_file(task_id)

    async def _save_upload_result_to_redis(self, task: UploadTask, upload_result):
        """
        Guarda los message_ids del upload en Redis para forwarding persistente
        
        Args:
            task: Tarea de upload completada
            upload_result: Resultado del upload con message_ids
        """
        try:
            recording_service = self._get_recording_service()
            if not recording_service or not recording_service.redis_service:
                logger.debug(f"Redis not available for {task.username}, skipping message_ids persistence")
                return
            
            # Obtener recording_id del task
            recording_id = task.recording_id
            if not recording_id:
                logger.warning(f"No recording_id found in task for {task.username}, skipping Redis save")
                return
            
            # Preparar message_ids para Redis - MANTENER FORMATO ORIGINAL
            if upload_result and upload_result is not True:
                # NO convertir a diccionario - mantener formato original para forwarding
                message_ids = upload_result
                
                # Guardar en Redis
                redis_service = recording_service.redis_service
                success = await redis_service.save_upload_result(
                    recording_id=recording_id,
                    message_ids=message_ids,
                    primary_chat_id=task.chat_id
                )
                
                if success:
                    logger.info(f"✅ Saved upload result to Redis for {task.username} (recording_id: {recording_id})")
                else:
                    logger.warning(f"⚠️ Failed to save upload result to Redis for {task.username}")
            else:
                logger.debug(f"No valid message_ids to save for {task.username}")
                
        except Exception as e:
            logger.error(f"❌ Error saving upload result to Redis for {task.username}: {e}")
    
    async def _check_and_finalize_fragmented_recording(self, recording_id: str, username: str, redis_service, recording_service):
        """
        Verifica si una grabación fragmentada ha terminado completamente y finaliza las entregas
        
        Args:
            recording_id: ID de la grabación
            username: Nombre del usuario de TikTok
            redis_service: Servicio Redis
            recording_service: Servicio de grabación
        """
        try:
            # Verificar si es una grabación fragmentada
            is_fragmented = await redis_service.is_fragmented_recording(recording_id)
            
            if not is_fragmented:
                logger.debug(f"Recording {recording_id} is not fragmented, skipping finalization")
                return

            # Verificar si hay fragmentos activos usando Redis (fuente de verdad persistente)
            logger.info(f"🔍 Checking for active fragments for {username} using Redis state")
            has_active_fragments = await redis_service._has_active_fragments(username)
            logger.info(f"🔍 Active fragments check result for {username}: {has_active_fragments}")

            if not has_active_fragments:
                logger.info(f"🔍 No active fragments detected for {username}, finalizing fragmented recording {recording_id}")
                
                # Marcar todos los fragmentos como completados
                success = await redis_service.mark_all_fragments_completed(recording_id)
                
                if success:
                    logger.info(f"✅ Marked all fragments completed for {recording_id}")
                    
                    # Finalizar entregas de todos los suscriptores y usuario primario
                    finalized = await redis_service.finalize_fragmented_recording_deliveries(recording_id)
                    
                    if finalized:
                        logger.info(f"🎉 Finalized all deliveries for fragmented recording {recording_id}")
                    else:
                        logger.warning(f"⚠️ Failed to finalize deliveries for fragmented recording {recording_id}")
                else:
                    logger.warning(f"⚠️ Failed to mark all fragments completed for {recording_id}")
            else:
                logger.info(f"⏸️ Active fragments still exist for {username}, not finalizing fragmented recording yet")
                    
        except Exception as e:
            logger.error(f"❌ Error checking and finalizing fragmented recording for {username}: {e}")
    
    async def _handle_shared_recording_forwarding_redis(self, task: UploadTask, upload_result):
        """
        Maneja el reenvío usando Redis como fuente de verdad para message_ids y suscriptores
        
        Args:
            task: Tarea de upload completada
            upload_result: Resultado del upload (usado como fallback)
        """
        try:
            recording_service = self._get_recording_service()
            if not recording_service:
                logger.warning(f"⚠️ RecordingService not found for {task.username}")
                return
            
            # Verificar Redis service
            redis_service = recording_service.redis_service
            if not redis_service:
                logger.warning(f"Redis service not available for {task.username}, using fallback")
                await self._handle_shared_recording_forwarding(task, upload_result)
                return
            
            # PRIMARY: Obtener recording_id desde Redis (más confiable)
            redis_recording_id = self._get_most_recent_recording_id(task.username)
            
            # VALIDACIÓN CRUZADA: Comparar Redis vs Task recording_id
            if redis_recording_id and task.recording_id:
                if redis_recording_id != task.recording_id:
                    logger.warning(f"⚠️ Recording ID mismatch for {task.username}: Redis={redis_recording_id}, Task={task.recording_id}")
                    logger.info(f"Using Task recording_id for consistency: {task.recording_id}")
                    recording_id = task.recording_id
                else:
                    logger.info(f"✅ Recording ID verified consistent: {redis_recording_id}")
                    recording_id = redis_recording_id
            elif redis_recording_id:
                recording_id = redis_recording_id
                logger.info(f"Using Redis recording_id: {redis_recording_id}")
            elif task.recording_id:
                recording_id = task.recording_id
                logger.info(f"Using Task recording_id as fallback: {task.recording_id}")
            else:
                logger.warning(f"Could not find recording_id for {task.username} (Redis + Task both failed)")
                return
            
            logger.info(f"🔄 Starting Redis-based forwarding for {task.username} (recording_id: {recording_id})")
            
            # 1. Marcar usuario primario como entregado
            await redis_service.mark_primary_delivered(recording_id)
            logger.info(f"✅ Primary user delivery confirmed for {recording_id}")
            
            # 2. Obtener message_ids desde Redis (fuente de verdad)
            upload_data = await redis_service.get_upload_result(recording_id)
            if not upload_data:
                logger.warning(f"No upload data found in Redis for {recording_id}, using fallback")
                # Fallback a upload_result original
                message_ids_for_forwarding = upload_result
            else:
                message_ids_for_forwarding = upload_data.get("message_ids", upload_result)
                logger.info(f"📋 Retrieved message_ids from Redis for {recording_id}")
            
            # 3. Obtener suscriptores pendientes
            subscriptions = await redis_service.get_pending_subscriptions(recording_id)
            logger.info(f"👥 Found {len(subscriptions)} pending subscribers for {recording_id}")
            
            if not subscriptions:
                logger.info(f"No pending subscribers for {recording_id}")

                # CRÍTICO: Verificar y finalizar grabación fragmentada ANTES del cleanup
                await self._check_and_finalize_fragmented_recording(recording_id, task.username, redis_service, recording_service)

                # Marcar forwarding como completado y cleanup
                await redis_service.mark_forwarding_completed(recording_id)
                
                # Limpiar registros de sesión del usuario (no hay suscriptores, proceso completo)
                deleted_sessions = await redis_service.cleanup_user_session_records(task.username, recording_id)
                if deleted_sessions > 0:
                    logger.info(f"🧹 Cleaned {deleted_sessions} session records for {task.username} (no subscribers)")
                
                await redis_service.cleanup_upload_data(recording_id)
                
                # Cleanup completo si no hay suscriptores
                can_cleanup = await redis_service.cleanup_completed_recording(recording_id)
                if can_cleanup:
                    recording_service._cleanup_memory_structures(task.username)
                    logger.info(f"🧹 Complete cleanup finished for {recording_id} (no subscribers)")
                return
            
            # 4. Forward a cada suscriptor usando message_ids desde Redis
            successful_forwards = 0
            for subscription in subscriptions:
                try:
                    user_id = int(subscription['user_id'])
                    chat_id = int(subscription['chat_id'])
                    
                    logger.info(f"📤 Forwarding to subscriber {user_id} (chat: {chat_id}) using Redis message_ids")
                    
                    # Forward usando message_ids desde Redis
                    forward_success = await self._telegram.forward_video_to_backup(
                        message_ids=message_ids_for_forwarding,
                        source_chat_id=task.chat_id,
                        backup_chat_id=chat_id,
                        username=task.username
                    )
                    
                    if forward_success:
                        await redis_service.mark_delivered(recording_id, user_id)
                        successful_forwards += 1
                        logger.info(f"✅ Successfully forwarded to subscriber {user_id}")
                    else:
                        # Incrementar contador de reintentos para este suscriptor
                        await redis_service.increment_retry_count(recording_id, user_id)
                        logger.warning(f"❌ Failed to forward to subscriber {user_id} (retry count incremented)")
                
                except Exception as subscriber_error:
                    logger.error(f"❌ Error forwarding to subscriber {subscription.get('user_id', 'unknown')}: {subscriber_error}")
                    continue
            
            logger.info(f"📊 Forwarding completed: {successful_forwards}/{len(subscriptions)} successful")
            
            # 4.5. CRÍTICO: Verificar si es una grabación fragmentada y finalizarla DESPUÉS del forwarding
            await self._check_and_finalize_fragmented_recording(recording_id, task.username, redis_service, recording_service)
            
            # 5. Marcar forwarding como completado
            await redis_service.mark_forwarding_completed(recording_id)
            
            # 5.5. Limpiar registros de sesión del usuario después de forwarding completo
            deleted_sessions = await redis_service.cleanup_user_session_records(task.username, recording_id)
            if deleted_sessions > 0:
                logger.info(f"🧹 Cleaned {deleted_sessions} session records for {task.username} after complete forwarding")
            
            # 6. Cleanup de datos de upload
            await redis_service.cleanup_upload_data(recording_id)
            
            # 7. Cleanup completo de la grabación
            can_cleanup = await redis_service.cleanup_completed_recording(recording_id)
            if can_cleanup:
                recording_service._cleanup_memory_structures(task.username)
                logger.info(f"🎯 COMPLETE cleanup finished for {recording_id}")
            else:
                logger.info(f"🔍 Some deliveries still pending for {recording_id}")
                
        except Exception as e:
            logger.error(f"❌ Error in Redis-based forwarding for {task.username}: {e}")
            # Fallback a método original
            await self._handle_shared_recording_forwarding(task, upload_result)
    
    async def _handle_shared_recording_forwarding(self, task: UploadTask, upload_result):
        """
        Maneja el reenvío de grabaciones compartidas a suscriptores usando Redis como fuente de verdad
        
        Flujo transaccional:
        1. Marcar usuario primario como entregado 
        2. Forward a suscriptores desde Redis
        3. Marcar cada entrega exitosa
        4. Cleanup completo solo cuando PRIMARY + TODOS los suscriptores confirmados
        
        Args:
            task: Tarea de upload que se completó exitosamente
            upload_result: Resultado del upload (message_ids o True)
        """
        try:
            logger.info(f"🔄 Starting shared recording forwarding for {task.username}")
            
            # Verificar que tenemos un resultado válido para reenviar
            if not upload_result or upload_result is True:
                logger.warning(f"Invalid upload result for forwarding {task.username}: {upload_result}")
                return
            
            recording_service = self._get_recording_service()
            if not recording_service:
                logger.warning(f"⚠️ RecordingService not found for forwarding {task.username}")
                return
            
            # PRIMARY: Obtener recording_id desde Redis (más confiable)
            redis_recording_id = self._get_most_recent_recording_id(task.username)
            
            # VALIDACIÓN CRUZADA: Comparar Redis vs Task recording_id
            if redis_recording_id and task.recording_id:
                if redis_recording_id != task.recording_id:
                    logger.warning(f"⚠️ Recording ID mismatch for {task.username}: Redis={redis_recording_id}, Task={task.recording_id}")
                    logger.info(f"Using Task recording_id for consistency: {task.recording_id}")
                    recording_id = task.recording_id
                else:
                    logger.info(f"✅ Recording ID verified consistent: {redis_recording_id}")
                    recording_id = redis_recording_id
            elif redis_recording_id:
                recording_id = redis_recording_id
                logger.info(f"Using Redis recording_id: {redis_recording_id}")
            elif task.recording_id:
                recording_id = task.recording_id
                logger.info(f"Using Task recording_id as fallback: {task.recording_id}")
            else:
                logger.warning(f"Could not find recording_id for {task.username} (Redis + Task both failed)")
                return
            
            # Obtener Redis service
            redis_service = recording_service.redis_service
            if not redis_service:
                logger.warning(f"Redis service not available, falling back to memory-only forwarding")
                # Fallback a la lógica original de memoria
                await self._handle_memory_forwarding(task, upload_result, recording_service)
                return
            
            logger.info(f"✅ Found RecordingService and Redis for {task.username} (recording_id: {recording_id})")
            
            # 1. MARCAR USUARIO PRIMARIO COMO ENTREGADO
            await redis_service.mark_primary_delivered(recording_id)
            logger.info(f"✅ Primary user delivery confirmed for {recording_id}")
            
            # 2. Obtener suscriptores pendientes desde REDIS (fuente de verdad)
            subscriptions = await redis_service.get_pending_subscriptions(recording_id)
            logger.info(f"📋 Found {len(subscriptions)} pending subscribers in Redis for {task.username}")
            
            if not subscriptions:
                logger.info(f"No pending subscribers found in Redis for {task.username}")
                # Intentar cleanup ya que no hay suscriptores pendientes
                can_cleanup = await redis_service.cleanup_completed_recording(recording_id)
                if can_cleanup:
                    # Limpiar memoria también
                    recording_service._cleanup_memory_structures(task.username)
                    logger.info(f"🧹 Complete cleanup finished for {recording_id} (no subscribers)")
                return
            
            # 3. Forward a cada suscriptor
            successful_forwards = 0
            for subscription in subscriptions:
                try:
                    user_id = int(subscription['user_id'])
                    chat_id = int(subscription['chat_id'])
                    
                    logger.info(f"📤 Forwarding {task.username} to subscriber {user_id} (chat: {chat_id})")
                    
                    # Forward usando método existente
                    forward_success = await self._telegram.forward_video_to_backup(
                        message_ids=upload_result,
                        source_chat_id=task.chat_id,  # Chat del usuario primario
                        backup_chat_id=chat_id,  # Chat del suscriptor
                        username=task.username
                    )
                    
                    if forward_success:
                        # CONFIRMAR ENTREGA EN REDIS
                        await redis_service.mark_delivered(recording_id, user_id)
                        successful_forwards += 1
                        logger.info(f"✅ Forward confirmed for subscriber {user_id} in Redis")
                    else:
                        logger.warning(f"❌ Failed to forward {task.username} to subscriber {user_id}")
                
                except Exception as subscriber_error:
                    logger.error(f"❌ Error forwarding to subscriber {subscription.get('user_id', 'unknown')}: {subscriber_error}")
                    continue
            
            logger.info(f"📊 Forwarding completed: {successful_forwards}/{len(subscriptions)} successful")
            
            # 4. CLEANUP COMPLETO solo si PRIMARY + TODOS los suscriptores confirmados
            can_cleanup = await redis_service.cleanup_completed_recording(recording_id)
            if can_cleanup:
                # Limpiar memoria también
                recording_service._cleanup_memory_structures(task.username)
                logger.info(f"🎯 COMPLETE cleanup finished for {recording_id}")
            else:
                logger.info(f"🔍 Some deliveries still pending for {recording_id}, keeping data")
                
        except Exception as e:
            logger.error(f"❌ Error in Redis-based shared recording forwarding for {task.username}: {e}")
            
            # Fallback a forwarding de memoria en caso de error crítico
            try:
                recording_service = self._get_recording_service()
                if recording_service:
                    await self._handle_memory_forwarding(task, upload_result, recording_service)
            except Exception as fallback_error:
                logger.error(f"❌ Fallback forwarding also failed for {task.username}: {fallback_error}")
    
    async def _handle_memory_forwarding(self, task: UploadTask, upload_result, recording_service):
        """
        Fallback forwarding usando solo estructuras de memoria
        Usado cuando Redis no está disponible
        """
        try:
            logger.info(f"🔄 Using memory-only forwarding for {task.username}")
            
            # Obtener suscriptores de memoria
            subscribers = recording_service._get_recording_subscribers(task.username)
            logger.info(f"📋 Found {len(subscribers) if subscribers else 0} subscribers in memory for {task.username}")
            
            if not subscribers:
                logger.info(f"No subscribers found in memory for {task.username}")
                # Limpiar memoria
                recording_service._cleanup_memory_structures(task.username)
                return
            
            # Forward a cada suscriptor
            for subscriber in subscribers:
                try:
                    subscriber_chat_id = subscriber['chat_id']
                    
                    forward_success = await self._telegram.forward_video_to_backup(
                        message_ids=upload_result,
                        source_chat_id=task.chat_id,
                        backup_chat_id=subscriber_chat_id,
                        username=task.username
                    )
                    
                    if forward_success:
                        logger.info(f"✅ Memory forward successful to subscriber {subscriber['user_id']}")
                    else:
                        logger.warning(f"❌ Memory forward failed to subscriber {subscriber['user_id']}")
                
                except Exception as subscriber_error:
                    logger.error(f"❌ Error in memory forward to subscriber {subscriber.get('user_id', 'unknown')}: {subscriber_error}")
                    continue
            
            # Limpiar memoria después del forwarding
            recording_service._cleanup_memory_structures(task.username)
            logger.info(f"🧹 Memory cleanup completed for {task.username}")
            
        except Exception as e:
            logger.error(f"❌ Error in memory-only forwarding for {task.username}: {e}")

    def _get_recording_service(self):
        """
        Obtiene la instancia del WorkerRecordingService usando el registro de clase
        """
        try:
            from services.worker_recording_service import WorkerRecordingService
            
            # Debug logging to understand singleton state
            logger.debug(f"🔍 Attempting to get WorkerRecordingService singleton")
            logger.debug(f"🔍 WorkerRecordingService._instance = {WorkerRecordingService._instance}")
            logger.debug(f"🔍 WorkerRecordingService._initialized = {WorkerRecordingService._initialized}")

            instance = WorkerRecordingService.get_instance()
            if instance:
                logger.debug(f"✅ Found WorkerRecordingService instance via class registry: {type(instance).__name__}")
                return instance
            else:
                logger.warning("⚠️ WorkerRecordingService instance not registered")
                logger.debug("🔍 This usually means WorkerRecordingService was not properly initialized")
                return None
            
        except Exception as e:
            logger.error(f"❌ Error getting WorkerRecordingService instance: {e}")
            return None

    def _safe_decode(self, value) -> str:
        """
        Decodifica de forma segura bytes a string, o retorna string si ya está decodificado
        """
        if isinstance(value, bytes):
            return value.decode('utf-8')
        return str(value)

    def _get_most_recent_recording_id(self, username: str) -> Optional[str]:
        """
        Obtiene el recording_id de la grabación más reciente para el usuario desde Redis
        usando timestamp-based discovery
        
        Args:
            username: Nombre de usuario de TikTok
            
        Returns:
            recording_id de la grabación más reciente o None si no se encuentra
        """
        try:
            recording_service = self._get_recording_service()
            if not recording_service or not recording_service.redis_service:
                logger.warning(f"Redis service not available for timestamp-based discovery: {username}")
                return None
            
            redis_client = recording_service.redis_service.redis
            
            # Buscar todos los registros recording:username:*
            redis_keys = redis_client.keys(f"recording:{username}:*")
            
            if not redis_keys:
                logger.warning(f"No Redis recording keys found for {username}")
                return None
            
            # Ordenar por timestamp (extraer timestamp del key y encontrar el más reciente)
            # Formato del key: recording:username:timestamp
            def extract_timestamp(key):
                key_str = self._safe_decode(key)
                return int(key_str.split(':')[-1])
            
            latest_key = max(redis_keys, key=extract_timestamp)
            
            # Obtener recording_id del registro más reciente
            recording_data = redis_client.hgetall(latest_key)
            if not recording_data:
                logger.warning(f"Empty recording data for key {self._safe_decode(latest_key)}")
                return None
            
            # Buscar recording_id de forma robusta (maneja tanto bytes como strings)
            recording_id = None
            for key, value in recording_data.items():
                key_str = self._safe_decode(key)
                if key_str == 'recording_id':
                    recording_id = self._safe_decode(value)
                    break
            
            if not recording_id:
                logger.warning(f"No recording_id field found in Redis data for {self._safe_decode(latest_key)}")
                return None
            
            logger.info(f"📍 Found most recent recording_id via timestamp: {recording_id} for {username}")
            return recording_id
            
        except Exception as e:
            logger.error(f"❌ Error getting most recent recording_id for {username}: {e}")
            return None

    def _is_recording_still_active(self, recording_service, username: str) -> bool:
        """
        Verifica si aún hay una grabación activa para el username
        Esto ayuda a distinguir entre fragmentos intermedios y el fragmento final
        
        IMPORTANTE: Usa master_recordings en lugar de user_recordings porque
        user_recordings se limpia en cleanup_callback antes del forwarding
        """
        try:
            # Usar las estructuras compartidas que se preservan hasta después del forwarding
            with recording_service._recording_lock:
                # Si existe en master_recordings, la grabación aún no ha terminado completamente
                if username in recording_service.master_recordings:
                    logger.debug(f"🔍 Found active shared recording for {username} in master_recordings")
                    return True
                    
            logger.debug(f"🔍 No active shared recording found for {username}")
            return False
            
        except Exception as e:
            logger.error(f"Error checking if recording is still active for {username}: {e}")
            # En caso de error, asumir que es el fragmento final para limpiar
            return False

    def _check_and_cleanup_file(self, task_id: str):
        """
        Verifica si debe eliminar el archivo según las reglas:
        1. Si backup está deshabilitado: limpiar cuando main upload complete
        2. Si backup está habilitado: limpiar cuando main upload complete Y backup complete (exitoso o fallido)
        """
        if task_id not in self._completed_tasks:
            return
            
        task = self._completed_tasks[task_id]
        
        # Determinar si debe limpiar el archivo
        should_cleanup = False
        
        if not task.backup_upload_enabled:
            # Sin backup: limpiar cuando main upload complete
            should_cleanup = task.main_upload_completed
        else:
            # Con backup: limpiar cuando AMBOS estén completados (exitoso o fallido)
            should_cleanup = task.main_upload_completed and task.backup_upload_completed
        
        if should_cleanup:
            logger.info(f"Upload process completed for {task.username} - cleaning up files")
            
            # Eliminar archivo principal
            try:
                if os.path.exists(task.file_path):
                    os.remove(task.file_path)
                    logger.info(f"Deleted main file: {task.file_path}")
            except Exception as e:
                logger.warning(f"Could not delete main file {task.file_path}: {e}")
            
            # Eliminar collage si existe
            try:
                if task.collage_path and os.path.exists(task.collage_path):
                    os.remove(task.collage_path)
                    logger.debug(f"Deleted collage file: {task.collage_path}")
            except Exception as e:
                logger.warning(f"Could not delete collage {task.collage_path}: {e}")
            
            # Eliminar thumbnail si existe
            try:
                if task.thumbnail_path and os.path.exists(task.thumbnail_path):
                    os.remove(task.thumbnail_path)
                    logger.debug(f"Deleted thumbnail file: {task.thumbnail_path}")
            except Exception as e:
                logger.warning(f"Could not delete thumbnail {task.thumbnail_path}: {e}")
            
            # Limpiar carpeta de grabación si está usando carpetas organizadas
            self._cleanup_recording_folder(task.file_path)
            
            # Limpiar archivo de estado de sesión si existe
            self._cleanup_session_state_file(task.file_path, task.username)
            
            # Remover del tracking
            del self._completed_tasks[task_id]

    def _cleanup_recording_folder(self, file_path: str):
        """
        Limpia la carpeta de grabación después del upload exitoso si está usando carpetas organizadas
        """
        try:
            # Solo proceder si las carpetas organizadas están habilitadas
            if not config.enable_organized_folders:
                return
                
            video_dir = os.path.dirname(file_path)
            base_output = config.output_directory
            
            # Asegurar que estamos dentro de una subcarpeta organizada, no en el directorio base
            if os.path.normpath(video_dir) == os.path.normpath(base_output):
                return
            
            # Verificar si la carpeta está vacía
            if os.path.exists(video_dir):
                try:
                    folder_contents = os.listdir(video_dir)
                    
                    if not folder_contents:
                        # La carpeta está vacía, eliminarla
                        os.rmdir(video_dir)
                        logger.info(f"🗑️ Cleaned up empty recording folder after upload: {video_dir}")
                    else:
                        # La carpeta tiene archivos, listar en debug pero no eliminar
                        logger.debug(f"Recording folder not empty after upload, keeping: {video_dir} (contents: {folder_contents})")
                        
                except OSError as e:
                    # Puede fallar si otro proceso está usando la carpeta
                    logger.debug(f"Could not list or remove folder {video_dir}: {e}")
                    
        except Exception as e:
            logger.warning(f"Error during recording folder cleanup: {e}")
    
    def _cleanup_session_state_file(self, file_path: str, username: str):
        """
        Elimina el archivo de estado de sesión después de un upload exitoso
        """
        try:
            # Buscar el archivo de estado de sesión correspondiente
            recording_states_dir = os.path.join(config.output_directory, "recording_states")
            
            if not os.path.exists(recording_states_dir):
                return
                
            # Buscar archivo de sesión que coincida con el usuario
            for filename in os.listdir(recording_states_dir):
                if not filename.startswith("session_") or not filename.endswith(".json"):
                    continue
                    
                session_file = os.path.join(recording_states_dir, filename)
                
                try:
                    import json
                    with open(session_file, 'r', encoding='utf-8') as f:
                        session_data = json.load(f)
                    
                    # Verificar si es la sesión correcta
                    if session_data.get('user') == username:
                        # Solo eliminar si está marcado como completed
                        if session_data.get('status') == 'completed':
                            os.remove(session_file)
                            logger.info(f"🗑️ Session state file deleted after successful upload: {username}")
                            break
                        
                except Exception as e:
                    logger.debug(f"Error reading session file {session_file}: {e}")
                    continue
                    
        except Exception as e:
            logger.warning(f"Error during session state cleanup for {username}: {e}")
    
    def _mark_recovery_file_as_uploaded(self, file_path: str):
        """
        Marca un archivo como exitosamente recuperado y subido
        """
        try:
            from utils.upload_recovery import mark_file_recovered
            mark_file_recovered(file_path)
        except Exception as e:
            logger.debug(f"Could not mark recovery file as uploaded {file_path}: {e}")
    
    async def _cleanup_failed_upload(self, task: UploadTask, error_type: UploadError):
        """Limpia los archivos y recursos después de un fallo de carga DEFINITIVO del upload principal"""
        try:
            # Marcar el main upload como fallido definitivamente
            task.main_upload_completed = True  # Completado (aunque fallido)
            task_id = f"{task.username}_{task.fragment_number}_{task.file_path}"
            
            # LIMPIAR REDIS si el upload primario falló definitivamente
            await self._cleanup_failed_primary_upload_redis(task)
            
            # Verificar si debe limpiar según las reglas normales
            self._check_and_cleanup_file(task_id)
                    
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    async def _cleanup_failed_primary_upload_redis(self, task: UploadTask):
        """
        Limpia datos de Redis cuando falla el upload principal definitivamente
        
        Args:
            task: Tarea de upload que falló
        """
        try:
            recording_service = self._get_recording_service()
            if not recording_service or not recording_service.redis_service:
                logger.debug(f"Redis not available for cleanup of failed upload: {task.username}")
                return
            
            # Obtener recording_id
            recording_data = recording_service.master_recordings.get(task.username)
            if not recording_data:
                logger.debug(f"No recording data found for failed upload cleanup: {task.username}")
                return
            
            recording_id = recording_data.get('recording_id')
            if not recording_id:
                logger.debug(f"No recording_id found for failed upload cleanup: {task.username}")
                return
            
            redis_service = recording_service.redis_service
            
            # Marcar upload primario como fallido
            await redis_service.mark_primary_upload_failed(recording_id)
            
            # Cleanup completo ya que no hay video para hacer forward
            logger.warning(f"❌ Primary upload failed for {task.username}, cleaning entire recording from Redis")
            success = await redis_service._force_cleanup_recording(recording_id, reason="primary_upload_failed")
            
            if success:
                # Limpiar memoria también
                recording_service._cleanup_memory_structures(task.username)
                logger.info(f"🧹 Cleaned Redis data for failed primary upload: {task.username}")
            
        except Exception as e:
            logger.error(f"❌ Error cleaning Redis after failed primary upload for {task.username}: {e}")
    
    async def _increment_primary_upload_attempts_redis(self, task: UploadTask):
        """
        Incrementa el contador de intentos de upload en Redis
        
        Args:
            task: Tarea de upload que va a reintentarse
        """
        try:
            recording_service = self._get_recording_service()
            if not recording_service or not recording_service.redis_service:
                return
            
            # Obtener recording_id
            recording_data = recording_service.master_recordings.get(task.username)
            if not recording_data:
                return
            
            recording_id = recording_data.get('recording_id')
            if not recording_id:
                return
            
            redis_service = recording_service.redis_service
            
            # Incrementar contador de intentos
            current_attempts = await redis_service.increment_primary_upload_attempts(recording_id)
            logger.debug(f"🔄 Primary upload attempt {current_attempts} for {task.username}")
            
        except Exception as e:
            logger.error(f"❌ Error incrementing upload attempts in Redis for {task.username}: {e}")
    
    async def _handle_upload_error(self, task: UploadTask, error: Exception) -> bool:
        """Maneja los errores de carga y decide si reintentar"""
        error_type = UploadError.from_exception(error)
        task.error_type = error_type
        
        if error_type in self.FATAL_ERRORS:
            logger.error(f"Fatal error uploading {task.username}: {error}")
            self._upload_stats['fatal_errors'] += 1
            await self._cleanup_failed_upload(task, error_type)
            return False
            
        if self._should_retry(error_type, task.retry_count):
            task.retry_count += 1
            self._upload_stats['retried'] += 1
            
            # Incrementar contador en Redis para tracking
            await self._increment_primary_upload_attempts_redis(task)
            
            logger.warning(
                f"Retrying upload for {task.username} "
                f"(attempt {task.retry_count}/{task.max_retries}): {error}"
            )
            # Reintentar con backoff exponencial
            await asyncio.sleep(2 ** task.retry_count)
            
            # DEDUPLICATION: Re-añadir a tracking al reintentar
            self._processing_files.discard(task.file_path)  # Ya no está siendo procesado
            self._queued_files.add(task.file_path)  # Vuelve a la cola
            
            await self._queue.put(task)
            return True
            
        logger.error(f"Failed to upload {task.username} after {task.retry_count} attempts: {error}")
        self._upload_stats['failed'] += 1
        return False
    
    async def _upload_worker(self, worker_id: int = 0):
        """PHASE 2: Worker idéntico que procesa de la misma cola (with worker_id for logging)"""
        logger.info(f"🚀 Upload worker {worker_id} started")
        while True:
            try:
                # Obtener la siguiente tarea usando scheduler inteligente
                task = await self._get_next_task_smart()
                try:
                    # DEDUPLICATION: Marcar archivo como en proceso
                    self._queued_files.discard(task.file_path)  # Ya no está en cola
                    self._processing_files.add(task.file_path)  # Ahora está siendo procesado
                    
                    if not self._telegram:
                        raise RuntimeError("Telegram instance not set")
                    
                    # Verificar que el archivo sigue existiendo
                    if not os.path.exists(task.file_path):
                        raise FileNotFoundError(f"File not found: {task.file_path}")
                    
                    # Verificar tamaño del archivo antes de intentar subirlo
                    file_size = os.path.getsize(task.file_path)
                    if file_size == 0:
                        raise Exception("Empty file")  # Será convertido a FILE_CORRUPTED por UploadError.from_exception
                    
                    upload_result = await self._telegram.upload(
                        task.file_path,
                        task.chat_id,
                        task.username,
                        task.fragment_number,
                        task.is_fragmented,
                        collage_path=task.collage_path,
                        thumbnail_path=task.thumbnail_path
                    )
                    
                    logger.info(f"📤 Upload result for {task.username}: {upload_result} (type: {type(upload_result)})")
                    
                    # upload_result puede ser True (legacy) o message_id (int) para forwarding
                    success = upload_result is not False and upload_result is not None
                    
                    if success:
                        self._upload_stats['success'] += 1
                        logger.info(f"Successfully uploaded video for {task.username}")
                        
                        # Marcar upload principal como completado
                        task.main_upload_completed = True
                        task_id = f"{task.username}_{task.fragment_number}_{task.file_path}"
                        
                        # GUARDAR MESSAGE_IDS EN REDIS antes del forwarding
                        await self._save_upload_result_to_redis(task, upload_result)
                        
                        # Manejar backup upload si está habilitado
                        await self._handle_backup_upload(task, upload_result, task_id)
                        
                        # SHARED RECORDINGS: Manejar reenvío a suscriptores usando Redis
                        logger.info(f"🔄 About to call shared recording forwarding for {task.username}")
                        await self._handle_shared_recording_forwarding_redis(task, upload_result)
                        
                        # Marcar como recuperado si fue un archivo de recovery
                        self._mark_recovery_file_as_uploaded(task.file_path)
                        
                        # DEDUPLICATION: Limpiar tracking al completar exitosamente
                        self._processing_files.discard(task.file_path)
                        
                        # Verificar si debe limpiar archivos
                        self._check_and_cleanup_file(task_id)
                        
                        # DELAY POST-UPLOAD: Esperar antes del siguiente upload
                        if config.upload_delay_enabled:
                            delay_seconds = self._calculate_post_upload_delay(task, success=True)
                            if delay_seconds > 0:
                                logger.debug(f"⏳ Post-upload delay: {delay_seconds}s for {task.username}")
                                await asyncio.sleep(delay_seconds)
                    else:
                        await self._handle_upload_error(
                            task, 
                            Exception("Upload returned False")
                        )
                        
                except Exception as e:
                    error_will_retry = await self._handle_upload_error(task, e)
                    if not error_will_retry:
                        # Si no se va a reintentar, limpiar
                        await self._cleanup_failed_upload(task, UploadError.from_exception(e))
                        
                        # DELAY POST-ERROR: Esperar más tiempo después de errores
                        if config.upload_delay_enabled:
                            delay_seconds = self._calculate_post_upload_delay(task, success=False)
                            if delay_seconds > 0:
                                logger.debug(f"⏳ Post-error delay: {delay_seconds}s after failed upload for {task.username}")
                                await asyncio.sleep(delay_seconds)
                finally:
                    # DEDUPLICATION: Siempre limpiar tracking al finalizar (exitoso o fallido)
                    self._processing_files.discard(task.file_path)
                    self._queue.task_done()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in upload worker: {e}")
                await asyncio.sleep(1)  # Prevenir bucle infinito en caso de error
    
    async def _periodic_recovery_worker(self):
        """
        Worker periódico que ejecuta recovery de uploads pendientes cada X minutos
        Previene que archivos se queden en disco indefinidamente
        """
        if not config.upload_recovery_scan_enabled:
            logger.info("📴 Periodic upload recovery disabled by configuration")
            return
            
        recovery_interval = config.upload_recovery_interval_minutes * 60  # Convertir a segundos
        logger.info(f"🔄 Periodic upload recovery worker started (interval: {config.upload_recovery_interval_minutes} minutes)")
        
        while True:
            try:
                await asyncio.sleep(recovery_interval)
                
                current_time = time.time()
                time_since_last = current_time - self._last_recovery_time
                
                # Asegurar que no ejecutamos recovery muy frecuentemente
                if time_since_last >= (recovery_interval - 60):  # Con 1 minuto de tolerancia
                    logger.info(f"🔍 Starting periodic upload recovery scan...")
                    await self._recover_pending_uploads()
                    self._last_recovery_time = current_time
                else:
                    logger.debug(f"Skipping recovery scan - too soon since last scan ({time_since_last/60:.1f} minutes ago)")
                    
            except asyncio.CancelledError:
                logger.info("🛑 Periodic recovery worker cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in periodic recovery worker: {e}")
                # Continuar el loop a pesar del error
                await asyncio.sleep(60)  # Esperar 1 minuto antes de intentar de nuevo
    
    async def _recover_pending_uploads(self):
        """Recupera uploads que no se pudieron procesar debido a errores del sistema"""
        try:
            from utils.upload_recovery import scan_and_recover_pending_uploads, mark_file_recovered
            
            logger.info("🔍 Scanning for pending uploads to recover...")
            pending_files = scan_and_recover_pending_uploads()
            
            if not pending_files:
                logger.info("✅ No pending uploads found for recovery")
                return
                
            logger.info(f"📥 Found {len(pending_files)} files pending upload recovery")
            
            recovered_count = 0
            skipped_count = 0
            
            for file_info in pending_files:
                try:
                    # Obtener chat_id desde recording_states
                    chat_id = file_info.get('chat_id')
                    
                    if not chat_id:
                        logger.warning(f"⚠️ No chat_id found for recovery file: {file_info['username']} - {file_info['file_path']}")
                        skipped_count += 1
                        continue
                    
                    # Agregar a la cola de upload
                    await self.add_upload_task(
                        file_path=file_info['file_path'],
                        chat_id=chat_id,  # ✅ Ahora usa el chat_id correcto desde session state
                        username=file_info['username'],
                        fragment_number=file_info['part_number'],
                        is_fragmented=file_info['is_fragmented'],
                        collage_path=file_info['collage_path'],
                        thumbnail_path=file_info['thumbnail_path']
                    )
                    
                    # NO marcar como recuperado aquí - solo después del upload exitoso
                    
                    logger.info(f"📤 Added recovery upload: {file_info['username']} - {os.path.basename(file_info['file_path'])}")
                    recovered_count += 1
                    
                except Exception as e:
                    logger.error(f"❌ Error recovering file {file_info['file_path']}: {e}")
                    skipped_count += 1
            
            # Logging final del recovery
            if recovered_count > 0 or skipped_count > 0:
                logger.info(f"📊 Recovery completed: {recovered_count} files added to upload queue, {skipped_count} skipped")
            
            # REDIS FORWARDING RECOVERY: Recuperar forwarding pendiente
            await self._recover_pending_forwarding()
            
        except Exception as e:
            logger.error(f"❌ Error during upload recovery: {e}")
    
    async def _recover_pending_forwarding(self):
        """
        Recupera forwarding pendiente desde Redis al inicializar el bot
        """
        try:
            recording_service = self._get_recording_service()
            if not recording_service or not recording_service.redis_service:
                logger.debug("🚫 Redis service not available for forwarding recovery")
                return
            
            redis_service = recording_service.redis_service
            
            # Obtener grabaciones con forwarding pendiente
            pending_forwarding = await redis_service.get_pending_forwarding()
            
            if not pending_forwarding:
                logger.info("✅ No pending forwarding found for recovery")
                return
            
            logger.info(f"🔄 Found {len(pending_forwarding)} recordings with pending forwarding")
            
            recovered_count = 0
            failed_count = 0
            
            for recording_id in pending_forwarding:
                try:
                    # Obtener datos de upload desde Redis
                    upload_data = await redis_service.get_upload_result(recording_id)
                    if not upload_data:
                        logger.warning(f"⚠️ No upload data found in Redis for {recording_id}")
                        failed_count += 1
                        continue
                    
                    # Verificar que upload esté completado
                    upload_status = upload_data.get("upload_status")
                    if upload_status != "completed":
                        logger.debug(f"🔍 Upload not completed for {recording_id}, skipping forwarding recovery")
                        continue
                    
                    # Obtener message_ids
                    message_ids = upload_data.get("message_ids")
                    primary_chat_id = int(upload_data.get("primary_chat_id", 0))
                    
                    if not message_ids or not primary_chat_id:
                        logger.warning(f"⚠️ Invalid upload data for {recording_id}: missing message_ids or chat_id")
                        failed_count += 1
                        continue
                    
                    # Obtener suscriptores pendientes
                    subscriptions = await redis_service.get_pending_subscriptions(recording_id)
                    if not subscriptions:
                        logger.info(f"👥 No pending subscribers for {recording_id}, marking forwarding as completed")
                        await redis_service.mark_forwarding_completed(recording_id)
                        await redis_service.cleanup_upload_data(recording_id)
                        continue
                    
                    logger.info(f"🔄 Recovering forwarding for {recording_id} to {len(subscriptions)} subscribers")
                    
                    # Recuperar forwarding para cada suscriptor
                    successful_forwards = 0
                    for subscription in subscriptions:
                        try:
                            user_id = int(subscription['user_id'])
                            chat_id = int(subscription['chat_id'])
                            
                            logger.info(f"📤 Recovery forwarding to subscriber {user_id} (chat: {chat_id})")
                            
                            # Forward usando message_ids desde Redis
                            forward_success = await self._telegram.forward_video_to_backup(
                                message_ids=message_ids,
                                source_chat_id=primary_chat_id,
                                backup_chat_id=chat_id,
                                username=recording_id.split('_')[0]  # Extract username from recording_id
                            )
                            
                            if forward_success:
                                await redis_service.mark_delivered(recording_id, user_id)
                                successful_forwards += 1
                                logger.info(f"✅ Recovery forwarding successful for subscriber {user_id}")
                            else:
                                # Incrementar contador de reintentos en recovery también
                                await redis_service.increment_retry_count(recording_id, user_id)
                                logger.warning(f"❌ Recovery forwarding failed for subscriber {user_id} (retry count incremented)")
                        
                        except Exception as subscriber_error:
                            logger.error(f"❌ Error in recovery forwarding to subscriber {subscription.get('user_id', 'unknown')}: {subscriber_error}")
                            continue
                    
                    logger.info(f"📊 Recovery forwarding for {recording_id}: {successful_forwards}/{len(subscriptions)} successful")
                    
                    # Marcar forwarding como completado
                    await redis_service.mark_forwarding_completed(recording_id)
                    
                    # Cleanup de datos de upload
                    await redis_service.cleanup_upload_data(recording_id)
                    
                    # Cleanup completo de la grabación
                    can_cleanup = await redis_service.cleanup_completed_recording(recording_id)
                    if can_cleanup:
                        logger.info(f"🧹 Recovery cleanup finished for {recording_id}")
                    
                    recovered_count += 1
                    
                except Exception as e:
                    logger.error(f"❌ Error recovering forwarding for {recording_id}: {e}")
                    failed_count += 1
                    continue
            
            # Logging final del recovery de forwarding
            if recovered_count > 0 or failed_count > 0:
                logger.info(f"📊 Forwarding recovery completed: {recovered_count} recordings processed, {failed_count} failed")
                
        except Exception as e:
            logger.error(f"❌ Error during forwarding recovery: {e}")
    
    """
    DISABLED: Redis cleanup worker causing subscription deletion issues
    
    async def _periodic_cleanup_worker(self):
        Worker periódico que ejecuta limpieza automática de Redis
        
        Ejecuta:
        - Opción A: Limpieza por timeout (grabaciones > 3 horas)
        - Opción B: Limpieza por reintentos fallidos (> 3 fallos consecutivos)
        cleanup_interval = config.redis_cleanup_interval_minutes * 60  # Convertir a segundos
        
        logger.info(f"🧹 Redis cleanup worker started (interval: {config.redis_cleanup_interval_minutes} minutes)")
        
        while True:
            try:
                # Esperar el intervalo configurado
                await asyncio.sleep(cleanup_interval)
                
                current_time = time.time()
                
                # Solo proceder si ha pasado suficiente tiempo
                if current_time - self._last_cleanup_time < (cleanup_interval - 60):  # Buffer de 1 minuto
                    continue
                
                recording_service = self._get_recording_service()
                if not recording_service or not recording_service.redis_service:
                    logger.debug("🚫 Redis service not available for cleanup")
                    continue
                
                redis_service = recording_service.redis_service
                
                logger.info(f"🧹 Starting periodic Redis cleanup (timeout: {config.recording_timeout_hours}h, max_retries: {config.forwarding_max_retry_attempts})")
                
                # OPCIÓN A: Limpieza por timeout (grabaciones > 3 horas)
                expired_cleaned = await redis_service.cleanup_expired_recordings()
                
                # OPCIÓN B: Limpieza por reintentos fallidos de forwarding (> 3 fallos consecutivos)
                failed_forwarding_cleaned = await redis_service.cleanup_failed_forwarding()
                
                # OPCIÓN C: Limpieza por upload primario fallido (> 3 intentos)
                failed_primary_cleaned = await redis_service.cleanup_failed_primary_uploads()
                
                # OPCIÓN D: Limpieza de grabaciones huérfanas (sin proceso real)
                orphaned_cleaned = await redis_service.cleanup_orphaned_recordings()
                
                total_cleaned = expired_cleaned + failed_forwarding_cleaned + failed_primary_cleaned + orphaned_cleaned
                
                if total_cleaned > 0:
                    logger.info(f"🧹 Periodic cleanup completed: {expired_cleaned} expired + {failed_forwarding_cleaned} failed forwarding + {failed_primary_cleaned} failed primary + {orphaned_cleaned} orphaned = {total_cleaned} total cleaned")
                else:
                    logger.debug(f"🔍 Periodic cleanup completed: no recordings needed cleanup")
                
                # Obtener estadísticas para logging
                cleanup_stats = await redis_service.get_cleanup_stats()
                if cleanup_stats:
                    logger.info(f"📊 Cleanup stats: {cleanup_stats['total_pending_recordings']} pending, {cleanup_stats['expired_recordings']} expired, {cleanup_stats['failed_forwarding_recordings']} failed")
                
                self._last_cleanup_time = current_time
                
            except asyncio.CancelledError:
                logger.info("🛱 Redis cleanup worker cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in periodic cleanup worker: {e}")
                # Continuar con el loop incluso si hay errores
                await asyncio.sleep(60)  # Esperar 1 minuto antes de reintentar
                continue
        
        logger.info("🛱 Redis cleanup worker stopped")
    """
    
    async def _get_next_task_smart(self) -> UploadTask:
        """
        FIFO task scheduler compatible with multiple workers
        
        Processing order: First-in-first-out (arrival order)
        
        Returns:
            UploadTask: The next task (first in queue)
        """
        try:
            # FIFO: First-in-first-out task retrieval compatible with multiple workers  
            task = await self._queue.get()
            logger.debug(f"📦 Processing task: {task.username} ({task.file_size/1024/1024:.1f}MB, queued: {time.time() - task.enqueue_time:.1f}s ago)")
            return task
        except Exception as e:
            logger.error(f"❌ Error getting task from queue: {e}")
            raise
    
    def _calculate_post_upload_delay(self, task: UploadTask, success: bool) -> int:
        """
        Calcula el delay inteligente después de un upload basado en:
        - Si fue exitoso o falló
        - Tamaño del archivo
        - Configuración del sistema
        """
        if not config.upload_delay_enabled:
            return 0
        
        # Delay base diferente según el resultado
        if success:
            # Upload exitoso
            if task.is_large_file:
                # Archivos grandes necesitan más tiempo
                delay = config.upload_delay_large_files_seconds
                logger.debug(f"📦 Using large file delay: {delay}s for {task.username} ({task.file_size/1024/1024:.1f}MB)")
            else:
                # Archivos normales
                delay = config.upload_delay_between_tasks_seconds
                logger.debug(f"📄 Using normal delay: {delay}s for {task.username} ({task.file_size/1024/1024:.1f}MB)")
        else:
            # Upload falló
            delay = config.upload_delay_after_error_seconds
            logger.debug(f"❌ Using error delay: {delay}s for {task.username}")
        
        # Verificar si hay cola de emergencia (reducir delay si hay tareas urgentes)
        queue_size = self.get_queue_size()
        if queue_size > 20:  # Si hay muchas tareas pendientes
            reduced_delay = max(1, delay // 2)  # Reducir a la mitad, mínimo 1 segundo
            logger.debug(f"🚨 Reducing delay due to queue pressure: {delay}s -> {reduced_delay}s (queue: {queue_size})")
            return reduced_delay
        
        return delay

    def get_queue_health_status(self) -> dict:
        """
        Obtiene información detallada sobre el estado de la cola para diagnóstico
        """
        try:
            return {
                'queue_size': self.get_queue_size(),
                'capacity_usage_percent': round(self.get_queue_capacity_usage() * 100, 1),
                'at_capacity_threshold': self.is_queue_at_capacity_threshold(),
                'queued_files_count': len(self._queued_files),
                'processing_files_count': len(self._processing_files),
                'upload_stats': self._upload_stats.copy(),
                'worker_running': len(self._worker_tasks) > 0 and any(not task.done() for task in self._worker_tasks),
                'active_workers_count': len([task for task in self._worker_tasks if not task.done()]),
                'total_workers_count': len(self._worker_tasks),
                'recovery_worker_running': self._recovery_worker_task is not None and not self._recovery_worker_task.done(),
                'last_recovery_time': self._last_recovery_time,
                'time_since_last_recovery_minutes': round((time.time() - self._last_recovery_time) / 60, 1) if self._last_recovery_time > 0 else None,
                'delay_config': {
                    'enabled': config.upload_delay_enabled,
                    'normal_delay_seconds': config.upload_delay_between_tasks_seconds,
                    'large_file_delay_seconds': config.upload_delay_large_files_seconds,
                    'error_delay_seconds': config.upload_delay_after_error_seconds
                }
            }
        except Exception as e:
            logger.error(f"Error getting queue health status: {e}")
            return {'error': str(e)}

