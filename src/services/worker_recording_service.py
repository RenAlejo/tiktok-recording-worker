"""
WorkerRecordingService simplificado para workers independientes.
Mantiene solo la lógica esencial de grabación del RecordingService original
sin dependencias del bot server (admin, language, subscription).
"""

import threading
import time
import asyncio
import queue
import json
from queue import Empty
from typing import Dict, Optional, Set, Any
from utils.logger_manager import logger
from utils.utils import read_cookies
from core.tiktok_recorder import TikTokRecorder
from utils.enums import Mode
from interfaces.recording_interface import RecordingInterface
from utils.enums import TikTokError
from utils.custom_exceptions import LiveNotFound, ArgsParseError, \
    UserLiveException, IPBlockedByWAF, TikTokException, TikTokLiveRateLimitException
from core.tiktok_api import TikTokAPI
from config.env_config import config


class WorkerRecordingService(RecordingInterface):
    """
    Servicio de grabación simplificado para workers.
    Mantiene la lógica esencial del RecordingService original sin dependencias del bot.
    """

    # Singleton pattern (igual al original)
    _instance = None
    _initialized = False

    def __new__(cls, telegram_uploader=None, worker_id=None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls):
        """Obtiene la instancia actual del WorkerRecordingService"""
        return cls._instance

    def __init__(self, telegram_uploader=None, worker_id=None):
        if self._initialized:
            return
        self._initialized = True

        self.worker_id = worker_id or "unknown_worker"

        # Configuración del worker
        self.max_concurrent_jobs = config.worker_max_concurrent_jobs

        # Estructuras de datos idénticas al original
        self.user_recordings: Dict[int, Dict[str, dict]] = {}
        self.channel_recordings: Dict[int, Set[str]] = {}

        # Nuevas estructuras para grabación compartida (idénticas al original)
        self.master_recordings: Dict[str, dict] = {}  # Una grabación por username
        self.recording_subscribers: Dict[str, list] = {}  # Suscriptores por grabación
        self._recording_lock = threading.Lock()  # CRITICAL: El mismo lock del original

        self.telegram_uploader = telegram_uploader
        self.tiktok_api = TikTokAPI(cookies=read_cookies())

        # Redis service para persistencia (idéntico al original)
        self.redis_service = None
        if config.enable_recording_persistence:
            try:
                from services.recording_redis_service import RecordingRedisService
                self.redis_service = RecordingRedisService.get_instance()
                logger.info(f"✅ Redis persistence enabled for worker {self.worker_id}")
            except Exception as e:
                logger.error(f"❌ Failed to initialize Redis service for worker {self.worker_id}: {e}")
                logger.warning(f"⚠️ Worker {self.worker_id} continuing without Redis persistence")
        else:
            logger.info(f"📴 Redis persistence disabled for worker {self.worker_id}")

        # Inicializar cola de post-procesamiento
        self._initialize_postprocessing_queue()

        # Inicializar notificaciones Redis para comunicación bidireccional
        self._initialize_redis_notifications()

        logger.info(f"✅ WorkerRecordingService initialized for worker {self.worker_id}")

    def _initialize_postprocessing_queue(self):
        """Inicializa la cola de post-procesamiento (igual al original)"""
        try:
            from services.postprocessing_queue import postprocessing_queue
            self.postprocessing_queue = postprocessing_queue
            logger.info(f"✅ Post-processing queue initialized for worker {self.worker_id}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize post-processing queue for worker {self.worker_id}: {e}")
            self.postprocessing_queue = None

    def _initialize_redis_notifications(self):
        """Inicializa sistema de notificaciones Redis para comunicación bidireccional"""
        try:
            # Reutilizar conexión Redis existente si está disponible
            if self.redis_service and hasattr(self.redis_service, 'redis'):
                self.notification_redis = self.redis_service.redis
                logger.debug(f"Redis notifications initialized for worker {self.worker_id} (reusing existing connection)")
            else:
                # Crear conexión Redis dedicada para notificaciones si no existe
                import redis
                from config.env_config import config
                self.notification_redis = redis.Redis.from_url(
                    config.redis_url,
                    socket_timeout=60,  # Optimizado para Upstash cloud
                    socket_connect_timeout=30,  # Optimizado para Upstash cloud
                    socket_keepalive=True,  # Keepalive para cloud Redis
                    socket_keepalive_options={},
                    retry_on_timeout=True,  # Auto-retry en timeout
                    decode_responses=True
                )
                logger.debug(f"Redis notifications initialized for worker {self.worker_id} (new connection)")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Redis notifications for worker {self.worker_id}: {e}")
            self.notification_redis = None

    def _notify_bot_recording_event(self, event_type: str, job_id: str, username: str, user_id: int,
                                   error_message: str = None, metadata: dict = None):
        """
        Notifica al bot sobre eventos de grabación en tiempo real con retry automático

        Args:
            event_type: Tipo de evento ("recording_started", "recording_failed", "recording_stopped")
            job_id: ID del job asociado
            username: Nombre de usuario de TikTok
            user_id: ID de usuario de Telegram
            error_message: Mensaje de error si aplica
            metadata: Metadatos adicionales
        """
        if not self.notification_redis:
            logger.warning(f"⚠️ [{self.worker_id}] Redis notifications not available, skipping {event_type} notification for {username}")
            return

        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                notification = {
                    "type": event_type,
                    "job_id": job_id,
                    "username": username,
                    "user_id": user_id,
                    "worker_id": self.worker_id,
                    "timestamp": time.time(),
                    "metadata": metadata or {}
                }

                if error_message:
                    notification["error"] = error_message

                # Canal específico por usuario para notificaciones instantáneas (debe coincidir con bot)
                user_channel = f"bot:notifications:user_{user_id}"

                # Test Redis connection before publishing
                self.notification_redis.ping()

                # Publicar notificación
                result = self.notification_redis.publish(user_channel, json.dumps(notification))

                # Log solo para debugging si es necesario
                logger.debug(f"Sent {event_type} notification for {username} to user {user_id}")

                return  # Success, exit retry loop

            except redis.ConnectionError as e:
                logger.warning(f"⚠️ [{self.worker_id}] Redis connection error sending {event_type} notification (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    logger.error(f"❌ [{self.worker_id}] Failed to send {event_type} notification after {max_retries} attempts: {e}")

            except redis.TimeoutError as e:
                logger.warning(f"⏰ [{self.worker_id}] Redis timeout sending {event_type} notification (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"❌ [{self.worker_id}] Failed to send {event_type} notification after {max_retries} attempts: timeout")

            except Exception as e:
                logger.error(f"❌ [{self.worker_id}] Unexpected error sending {event_type} notification for {username} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    break

    def _notify_recording_started(self, job_id: str, username: str, user_id: int, recording_id: str = None):
        """Notifica que la grabación se inició exitosamente"""
        metadata = {
            "recording_id": recording_id,
            "estimated_duration": 3600,  # Estimación inicial
            "quality": "1080p"
        }
        self._notify_bot_recording_event("recording_started", job_id, username, user_id, metadata=metadata)

    def _notify_recording_failed(self, job_id: str, username: str, user_id: int, error_message: str):
        """Notifica que la grabación falló al iniciar"""
        # Notificar al bot
        self._notify_bot_recording_event("recording_failed", job_id, username, user_id, error_message=error_message)
        # Notificar a monitoring workers para que resuman monitoreo
        self._notify_monitoring_worker_failed(username, job_id, error_message)

    def _notify_recording_stopped(self, job_id: str, username: str, user_id: int):
        """Notifica que la grabación se detuvo"""
        self._notify_bot_recording_event("recording_stopped", job_id, username, user_id)

    def _create_master_recording(self, username: str, user_id: int, chat_id: int) -> Optional[str]:
        """Crea una grabación maestra (igual al original)"""
        try:
            recording_id = None
            if self.redis_service:
                from services.redis_helper import RedisAsyncHelper
                recording_id = RedisAsyncHelper.run_async_safe(
                    self.redis_service.create_recording_session,
                    username, user_id, chat_id,
                    timeout=5
                )

            # Crear en memoria AFTER Redis (or as fallback)
            with self._recording_lock:
                result_queue = queue.Queue()
                self.master_recordings[username] = {
                    'recording_id': recording_id,
                    'primary_user_id': user_id,
                    'primary_chat_id': chat_id,
                    'thread': None,
                    'start_time': time.time(),
                    'status': 'initializing',
                    'result_queue': result_queue
                }
                self.recording_subscribers[username] = []
                logger.info(f"Created master recording {recording_id} for {username} by user {user_id} (worker {self.worker_id})")
                return recording_id
        except Exception as e:
            logger.error(f"Error creating master recording for {username} (worker {self.worker_id}): {e}")
            return None

    def _is_recording_truly_active(self, username: str) -> bool:
        """Verifica si una grabación está verdaderamente activa (igual al original)"""
        with self._recording_lock:
            # Verificar en memoria primero
            if username not in self.master_recordings:
                return False

            master_data = self.master_recordings[username]
            thread = master_data.get('thread')

            # Si no hay thread o está muerto, no está activa
            if not thread or not thread.is_alive():
                logger.debug(f"Recording thread for {username} is not alive (worker {self.worker_id})")
                return False

            # Verificación adicional con Redis si está disponible
            if self.redis_service and master_data.get('recording_id'):
                try:
                    from services.redis_helper import RedisAsyncHelper
                    redis_active = RedisAsyncHelper.run_async_safe(
                        self.redis_service.is_recording_active,
                        master_data['recording_id'],
                        timeout=3
                    )

                    if not redis_active:
                        logger.debug(f"Recording {username} not active in Redis (worker {self.worker_id})")
                        return False
                except Exception as e:
                    logger.debug(f"Error checking Redis status for {username} (worker {self.worker_id}): {e}")
                    # Si Redis falla, confiar en verificación de memoria

            return True

    def start_recording(self, username: str, user_id: int, chat_id: int,
                       language: str = None, mode: Mode = None, job_id: str = None) -> bool:
        """
        Inicia una grabación (simplificado para workers)
        Workers no validan permisos - solo ejecutan jobs aprobados por el bot
        """
        try:
            logger.info(f"[{self.worker_id}] Starting recording for {username}")

            # Verificar si ya existe una grabación activa
            if self._is_recording_truly_active(username):
                logger.info(f"[{self.worker_id}] Recording already active for {username}")
                return True

            # VALIDACIÓN INMEDIATA antes de crear el thread
            validated_room_id = None
            try:
                cookies = read_cookies()
                validation_room_id = self.tiktok_api.get_room_id_from_user(username)
                if not validation_room_id:
                    error_message = f"User {username} is not live"
                    raise UserLiveException(error_message)
                elif not self.tiktok_api.is_room_alive(validation_room_id):
                    error_message = f"User {username} is not live"
                    raise UserLiveException(error_message)

                validated_room_id = validation_room_id

            except Exception as validation_error:
                # Limpiar cache del usuario para permitir reintentos inmediatos
                from utils.user_cache import UserCache
                user_cache = UserCache()
                user_cache.clear_user_cache(username)
                logger.error(f"[{self.worker_id}] User validation failed for {username}: {validation_error}")
                # NOTIFICACIÓN DE ERROR: Validación falló
                if job_id:
                    self._notify_recording_failed(job_id, username, user_id, f"Validation failed: {str(validation_error)}")
                raise UserLiveException(str(validation_error))

            # Crear grabación maestra después de validación exitosa
            recording_id = self._create_master_recording(username, user_id, chat_id)
            if not recording_id:
                error_msg = "Error creating recording. Please try again."
                # NOTIFICACIÓN DE ERROR: No se pudo crear recording
                if job_id:
                    self._notify_recording_failed(job_id, username, user_id, error_msg)
                raise UserLiveException(error_msg)

            def run_recording():
                try:
                    cookies = read_cookies()

                    def auto_restart_callback(next_fragment_number):
                        """Callback para reiniciar automáticamente la grabación con el siguiente fragmento"""
                        restart_thread = threading.Thread(
                            target=self._restart_recording_fragment,
                            args=(user_id, username, chat_id, next_fragment_number),
                            daemon=True
                        )
                        restart_thread.start()

                    recorder = TikTokRecorder(
                        url=None,
                        user=username,
                        room_id=validated_room_id,
                        mode=mode or Mode.MANUAL,
                        automatic_interval=config.automatic_interval_minutes,
                        cookies=cookies,
                        output=config.output_directory,
                        duration=None,
                        use_telegram=True,
                        telegram_chat_id=chat_id,
                        telegram_instance=self.telegram_uploader,
                        fragment_number=1,
                        auto_restart_callback=auto_restart_callback
                    )

                    # Set cleanup callback
                    def cleanup_callback():
                        logger.info(f"[{self.worker_id}] Cleanup callback called for {username}")
                        self._cleanup_recording(user_id, username)

                        # Notify monitoring workers that recording is complete
                        self._notify_recording_completed(username, job_id)

                    recorder.cleanup_callback = cleanup_callback

                    # Registrar el usuario como grabando
                    if user_id not in self.user_recordings:
                        self.user_recordings[user_id] = {}

                    self.user_recordings[user_id][username] = {
                        'recorder': recorder,
                        'thread': threading.current_thread(),
                        'start_time': time.time(),
                        'stop_event': threading.Event(),
                        'chat_id': chat_id,
                        'fragment_number': 1,
                        'recording_id': recording_id,
                        'original_user_id': user_id,
                        'original_username': username
                    }

                    if chat_id not in self.channel_recordings:
                        self.channel_recordings[chat_id] = set()
                    self.channel_recordings[chat_id].add(username)

                    # Helper para reportar al queue
                    def report_to_queue(success: bool, exception=None):
                        """Helper para reportar resultado al queue"""
                        result_queue = None
                        with self._recording_lock:
                            if username in self.master_recordings and 'result_queue' in self.master_recordings[username]:
                                result_queue = self.master_recordings[username]['result_queue']

                        if result_queue:
                            result_queue.put({'success': success, 'exception': exception})

                    # Definir callback que se ejecute cuando start_recording() sea exitoso
                    def on_recording_started():
                        """Callback que se ejecuta inmediatamente después de start_recording() exitoso"""
                        report_to_queue(True)

                        # NOTIFICACIÓN BIDIRECCIONAL: Notificar al bot inmediatamente
                        if job_id:
                            self._notify_recording_started(job_id, username, user_id, recording_id)
                        else:
                            logger.debug(f"No job_id available for notification - {username}")

                    # Asignar callback al recorder
                    recorder.on_recording_started_callback = on_recording_started

                    # Continuar con la grabación
                    recorder.run()

                except TikTokLiveRateLimitException as ex:
                    report_to_queue(False, ex)
                    logger.error(f"[{self.worker_id}] TikTokLive rate limit error for {username}: {ex}")
                    # NOTIFICACIÓN DE ERROR: Rate limit
                    if job_id:
                        self._notify_recording_failed(job_id, username, user_id, f"Rate limit error: {str(ex)}")
                    self._cleanup_recording(user_id, username)

                except (ArgsParseError, LiveNotFound, IPBlockedByWAF, UserLiveException, TikTokException) as ex:
                    report_to_queue(False, ex)
                    logger.error(f"[{self.worker_id}] Recording error for {username}: {ex}")
                    # NOTIFICACIÓN DE ERROR: Error conocido
                    if job_id:
                        self._notify_recording_failed(job_id, username, user_id, str(ex))
                    self._cleanup_recording(user_id, username)

                except Exception as e:
                    report_to_queue(False, e)
                    logger.error(f"[{self.worker_id}] Unexpected recording error for {username}: {e}")
                    # NOTIFICACIÓN DE ERROR: Error inesperado
                    if job_id:
                        self._notify_recording_failed(job_id, username, user_id, f"Unexpected error: {str(e)}")
                    self._cleanup_recording(user_id, username)

            recording_thread = threading.Thread(target=run_recording, daemon=True)
            recording_thread.start()

            # Actualizar el thread en la grabación maestra
            with self._recording_lock:
                if username in self.master_recordings:
                    self.master_recordings[username]['thread'] = recording_thread
                    self.master_recordings[username]['status'] = 'recording'

            # Esperar confirmación del callback del thread
            try:
                result_queue = None
                with self._recording_lock:
                    if username in self.master_recordings and 'result_queue' in self.master_recordings[username]:
                        result_queue = self.master_recordings[username]['result_queue']

                if result_queue:
                    logger.debug(f"[{self.worker_id}] Waiting for recording start confirmation for {username}...")
                    result = result_queue.get(timeout=30)

                    if result.get('success'):
                        logger.info(f"✅ [{self.worker_id}] Recording start confirmed for {username}")

                        # FAILSAFE: Enviar notificación directa como respaldo por si el callback falló
                        if job_id:
                            try:
                                self._notify_recording_started(job_id, username, user_id, recording_id)
                            except Exception as notify_error:
                                logger.error(f"❌ [{self.worker_id}] Failsafe notification failed for {username}: {notify_error}")

                        return True
                    else:
                        exception = result.get('exception')
                        if exception:
                            logger.error(f"[{self.worker_id}] Recording start failed for {username}: {exception}")
                            self._cleanup_recording(user_id, username)
                            raise exception
                        else:
                            logger.error(f"[{self.worker_id}] Recording start failed for {username}: Unknown error")
                            self._cleanup_recording(user_id, username)
                            raise UserLiveException("Recording failed to start. Please try again.")
                else:
                    # Fallback si no hay queue
                    logger.warning(f"[{self.worker_id}] No result queue found for {username}, using fallback delay")
                    time.sleep(config.bot_recording_init_wait)
                    return True

            except Empty:
                logger.error(f"❌ [{self.worker_id}] Recording start timeout for {username} - no confirmation received")
                self._cleanup_recording(user_id, username)
                raise UserLiveException("Recording failed to start (timeout). Please try again.")

        except Exception as e:
            logger.error(f"[{self.worker_id}] Error in start_recording: {e}")
            raise

    def stop_recording(self, username: str, user_id: int) -> bool:
        """
        Detiene una grabación (con parámetros adaptados para el worker)
        """
        try:
            logger.info(f"[{self.worker_id}] Stopping recording for {username}")

            # Llamar al método original con parámetros en el orden correcto
            return self._stop_recording_internal(user_id, username)

        except Exception as e:
            logger.error(f"[{self.worker_id}] Error stopping recording for {username}: {e}")
            return False

    def _stop_recording_internal(self, user_id: int, username: str) -> bool:
        """Método interno para detener grabación (igual al original)"""
        try:
            if user_id not in self.user_recordings or username not in self.user_recordings[user_id]:
                return False

            recording_info = self.user_recordings[user_id][username]
            recorder = recording_info['recorder']
            chat_id = recording_info.get('chat_id')

            recorder.stop_recording()
            time.sleep(5)

            # Limpiar la grabación del usuario
            if user_id in self.user_recordings and username in self.user_recordings[user_id]:
                del self.user_recordings[user_id][username]

            # Limpiar las grabaciones del canal
            if chat_id and chat_id in self.channel_recordings:
                self.channel_recordings[chat_id].discard(username)
                if not self.channel_recordings[chat_id]:
                    del self.channel_recordings[chat_id]

            return True

        except Exception as e:
            logger.error(f"[{self.worker_id}] Error al detener grabación: {e}")
            return False

    def _cleanup_recording(self, user_id: int, username: str):
        """Limpieza de grabación (simplificado del original)"""
        try:
            logger.debug(f"[{self.worker_id}] Cleaning up recording for {username}")

            # Limpiar estructuras de usuario
            if user_id in self.user_recordings and username in self.user_recordings[user_id]:
                recording_info = self.user_recordings[user_id][username]
                chat_id = recording_info.get('chat_id')

                del self.user_recordings[user_id][username]

                # Limpiar grabaciones del canal
                if chat_id and chat_id in self.channel_recordings:
                    self.channel_recordings[chat_id].discard(username)
                    if not self.channel_recordings[chat_id]:
                        del self.channel_recordings[chat_id]

            # NO limpiar master_recordings aquí - se maneja en UploadManager
            logger.debug(f"[{self.worker_id}] Cleanup completed for {username}")

        except Exception as e:
            logger.error(f"[{self.worker_id}] Error in cleanup for {username}: {e}")

    def _notify_recording_completed(self, username: str, job_id: str):
        """Notify monitoring workers that recording is complete"""
        try:
            if not self.notification_redis:
                logger.warning(f"[{self.worker_id}] Redis not available, cannot notify monitoring workers")
                return

            # Publish to monitoring_worker_broadcast channel
            notification = {
                "type": "recording_completed",
                "username": username,
                "job_id": job_id,
                "worker_id": self.worker_id,
                "timestamp": time.time(),
                "success": True
            }

            self.notification_redis.publish("monitoring_worker_broadcast", json.dumps(notification))
            logger.info(f"[{self.worker_id}] ✅ Notified monitoring workers: recording completed for {username}")

        except Exception as e:
            logger.error(f"[{self.worker_id}] Failed to notify monitoring workers: {e}")

    def _notify_monitoring_worker_failed(self, username: str, job_id: str, error_message: str):
        """Notify monitoring workers that recording failed"""
        try:
            if not self.notification_redis:
                logger.warning(f"[{self.worker_id}] Redis not available, cannot notify monitoring workers")
                return

            # Publish to monitoring_worker_broadcast channel
            notification = {
                "type": "recording_failed",
                "username": username,
                "job_id": job_id,
                "worker_id": self.worker_id,
                "timestamp": time.time(),
                "error": error_message,
                "success": False
            }

            self.notification_redis.publish("monitoring_worker_broadcast", json.dumps(notification))
            logger.info(f"[{self.worker_id}] ❌ Notified monitoring workers: recording failed for {username}")

        except Exception as e:
            logger.error(f"[{self.worker_id}] Failed to notify monitoring workers about failure: {e}")

    def _restart_recording_fragment(self, user_id: int, username: str, chat_id: int, fragment_number: int):
        """Reinicia grabación con nuevo fragmento (simplificado del original)"""
        logger.info(f"[{self.worker_id}] Restarting recording fragment {fragment_number} for {username}")

        # Implementación simplificada - delegar al sistema original si es necesario
        try:
            # Esta funcionalidad puede requerir más lógica del original si es necesaria
            pass
        except Exception as e:
            logger.error(f"[{self.worker_id}] Error restarting fragment for {username}: {e}")

    # Métodos de interfaz requeridos
    def can_start_recording(self, chat_id: int, username: str) -> bool:
        """Verifica si se puede iniciar una grabación específica en un canal"""
        if chat_id not in self.channel_recordings:
            return True
        return username not in self.channel_recordings[chat_id]

    def get_active_recordings(self, user_id: int) -> Dict[str, Dict]:
        """Obtiene grabaciones activas para un usuario específico"""
        return self.user_recordings.get(user_id, {})

    def get_active_recordings_count(self) -> int:
        """Obtiene el número total de grabaciones activas en el sistema"""
        total = 0
        for user_recordings in self.user_recordings.values():
            total += len(user_recordings)
        return total

    def get_heartbeat_recordings(self) -> Dict[str, Any]:
        """
        Obtiene datos de grabaciones para heartbeat del worker (escalable para gran escala).

        Returns:
            Dict con información detallada de grabaciones, capacidad y performance
        """
        try:
            with self._recording_lock:
                # Recopilar información de grabaciones activas
                active_recordings = []
                total_recordings = 0

                for user_id, user_recordings in self.user_recordings.items():
                    for username, recording_info in user_recordings.items():
                        total_recordings += 1

                        # Información básica de la grabación
                        recording_data = {
                            "username": username,
                            "user_id": user_id,
                            "chat_id": recording_info.get('chat_id'),
                            "started_at": recording_info.get('start_time', time.time()),
                            "fragment_number": recording_info.get('fragment_number', 1),
                            "recording_id": recording_info.get('recording_id'),
                            "status": "recording",
                            "duration": time.time() - recording_info.get('start_time', time.time())
                        }
                        active_recordings.append(recording_data)

                # Información de capacidad
                max_concurrent = self.max_concurrent_jobs
                current_active = total_recordings
                available_slots = max(0, max_concurrent - current_active)

                # Datos de performance simplificados (expandible en el futuro)
                performance_metrics = {
                    "total_recordings_today": total_recordings,  # Simplificado por ahora
                    "success_rate": 100.0,  # TODO: Implementar tracking real
                    "avg_recording_duration": 1800,  # TODO: Calcular promedio real
                    "last_error": None
                }

                # Sistema de información básico
                import psutil
                cpu_percent = psutil.cpu_percent(interval=0.1)
                memory_info = psutil.virtual_memory()

                heartbeat_data = {
                    "active_recordings": active_recordings,
                    "capacity": {
                        "max_concurrent": max_concurrent,
                        "current_active": current_active,
                        "available_slots": available_slots,
                        "utilization_percent": (current_active / max_concurrent) * 100 if max_concurrent > 0 else 0
                    },
                    "performance": performance_metrics,
                    "system_info": {
                        "cpu_usage_percent": cpu_percent,
                        "memory_usage_percent": memory_info.percent,
                        "memory_available_gb": memory_info.available / (1024**3),
                        "worker_id": self.worker_id
                    },
                    "master_recordings_count": len(self.master_recordings),
                    "timestamp": time.time()
                }

                logger.debug(f"[{self.worker_id}] Heartbeat data: {current_active} active recordings, {available_slots} available slots")
                return heartbeat_data

        except Exception as e:
            logger.error(f"[{self.worker_id}] Error generating heartbeat recordings data: {e}")
            # Retornar datos mínimos en caso de error
            return {
                "active_recordings": [],
                "capacity": {"max_concurrent": 5, "current_active": 0, "available_slots": 5, "utilization_percent": 0},
                "performance": {"success_rate": 0.0, "last_error": str(e)},
                "system_info": {"worker_id": self.worker_id},
                "master_recordings_count": 0,
                "timestamp": time.time()
            }