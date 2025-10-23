import os
import time
import threading
import asyncio
import subprocess
import signal
import re
from typing import Optional

try:
    import psutil
except ImportError:
    psutil = None

from http.client import HTTPException

from requests import RequestException

from core.tiktok_api import TikTokAPI
from utils.logger_manager import logger
from utils.video_management import VideoManagement
from utils.video_collage import VideoCollageGenerator
from upload.telegram import Telegram
from upload.upload_manager import UploadManager
from utils.custom_exceptions import LiveNotFound, UserLiveException, \
    TikTokException, TikTokLiveRateLimitException
from utils.enums import Mode, Error, TimeOut, TikTokError
from utils.realtime_video_sampler import RealtimeVideoSampler
from config.env_config import config


class TikTokRecorder:

    def __init__(
        self,
        url,
        user,
        room_id,
        mode,
        automatic_interval,
        cookies,
        output,
        duration,
        use_telegram,
        telegram_chat_id,
        telegram_instance,
        fragment_number=1,
        auto_restart_callback=None
    ):
        # Setup TikTok API client
        self.tiktok = TikTokAPI(cookies=cookies)

        # TikTok Data
        self.url = url
        self.user = user
        self.room_id = room_id

        # Tool Settings
        self.mode = mode
        self.automatic_interval = automatic_interval
        self.duration = duration
        self.output = output

        # Upload Settings
        self.use_telegram = use_telegram
        self.telegram_chat_id = telegram_chat_id
        self.telegram = telegram_instance
        self.upload_manager = UploadManager()
        self.upload_manager.set_telegram_instance(telegram_instance)
        self.cookies = cookies
        self.stop_flag = False
        
        # Fragment management
        self.fragment_number = fragment_number
        self.auto_restart_callback = auto_restart_callback
        self.max_file_size = config.max_file_size_bytes
        self.size_check_interval = config.size_check_interval_seconds
        self.last_size_check = 0
        self.cleanup_callback = None  # Will be set by RecordingService
        
        # Continuous recording management
        self.recording_parts = []  # Lista de partes de la misma sesión
        self.session_start_time = time.time()
        self.current_part_number = 1
        self.is_continuous_session = False
        self.corruption_restart_pending = False  # Flag para indicar que el próximo restart es por corrupción
        
        # Carpeta de sesión para grabaciones organizadas (reutilizar para todos los segmentos)
        self.session_folder_path = None
        
        # URL caching para evitar peticiones excesivas a TikTok
        self.cached_stream_url = None
        self.stream_url_cache_time = None
        self.stream_url_cache_duration = 1200  # Cache URL por 20 minutos (menor que expire time)
        
        # NUEVO: Archivo de estado persistente para grabaciones largas
        self.session_id = f"{self.user}_{int(self.session_start_time)}"
        self.state_file_path = None
        self._initialize_session_state_file()
        
        # Corruption detection and monitoring
        self.corruption_detected = False
        self.avcc_error_detected = False  # Flag específico para errores AVCC que requieren postprocesado
        self.url_expiration_detected = False  # Flag para errores de URL expirada (HTTP 404, etc.)
        self.matroska_corruption_detected = False  # Flag para corrupción de timestamp matroska
        self.ffmpeg_stderr_log = []
        self.corruption_monitor_thread = None
        self.corruption_threshold = 5  # Número de errores consecutivos para considerar corrupción
        self.corruption_count = 0
        self.last_corruption_time = 0
        self.stabilization_wait_time = 1  # Segundos para esperar estabilización
        self.ffmpeg_stderr_log_path = None
        
        # NUEVO: Límite de stderr log para prevenir acumulación excesiva de memoria
        self.max_stderr_log_entries = 1000  # Máximo 1000 entradas en memoria
        self.stderr_log_rotation_count = 0
        
        # FFmpeg Memory Guardian
        self.memory_monitor_thread = None
        self.memory_restart_triggered = False
        
        # Gap detection and trimming
        self.gap_detected = False
        self.gap_start_timestamp = None
        self.last_valid_timestamp = None
        self.last_timestamp_update = None
        
        # Real-time video sampling for silent corruption detection
        self.video_sampler = RealtimeVideoSampler(
            user=self.user,
            on_corruption_detected=self._handle_sampler_corruption_detected
        )

        # Check if the user's country is blacklisted
        self.check_country_blacklisted()

        # Get live information based on the provided user data
        if self.url:
            self.user, self.room_id = \
                self.tiktok.get_room_and_user_from_url(self.url)

        if not self.user:
            self.user = self.tiktok.get_user_from_room_id(self.room_id)

        # Solo obtener room_id si no se proporcionó uno válido
        if not self.room_id:
            logger.info(f"No room_id provided, fetching for user: {self.user}")
            self.room_id = self.tiktok.get_room_id_from_user(self.user)
        else:
            logger.info(f"Using provided room_id: {self.room_id} for user: {self.user}")

        logger.info(f"USERNAME: {self.user}")
        
        # Solo verificar si room está vivo si no se confía en la validación previa
        # (esto puede ser configurado si se necesita mayor control)
        if self.room_id:
            logger.info(f"ROOM_ID:  {self.room_id}")
        else:
            logger.error(f"No valid room_id found for user: {self.user}")

    def __del__(self):
        """
        Destructor para asegurar que se cierren las conexiones HTTP cuando el objeto es destruido
        """
        try:
            if hasattr(self, 'tiktok') and self.tiktok:
                self.tiktok.close()
                logger.debug(f"TikTok HTTP client closed in destructor for {getattr(self, 'user', 'unknown')}")
        except Exception as e:
            # No usar logger aquí ya que puede estar cerrado durante destrucción
            pass

    def run(self):
        """
        runs the program in the selected mode. 
        
        If the mode is MANUAL, it checks if the user is currently live and
        if so, starts recording.
        
        If the mode is AUTOMATIC, it continuously checks if the user is live
        and if not, waits for the specified timeout before rechecking.
        If the user is live, it starts recording.
        """
        if self.mode == Mode.MANUAL:
            self.manual_mode()

        if self.mode == Mode.AUTOMATIC:
            self.automatic_mode()

    def manual_mode(self):
        if not self.tiktok.is_room_alive(self.room_id):
            raise UserLiveException(
                f"{self.user}: {TikTokError.USER_NOT_CURRENTLY_LIVE}"
            )

        self.start_recording()

    def automatic_mode(self):
        while True:
            try:
                self.room_id = self.tiktok.get_room_id_from_user(self.user)
                self.manual_mode()

            except UserLiveException as ex:
                logger.info(ex)
                logger.info(f"Waiting {self.automatic_interval} minutes before recheck\n")
                time.sleep(self.automatic_interval * TimeOut.ONE_MINUTE)

            except ConnectionError:
                logger.error(Error.CONNECTION_CLOSED_AUTOMATIC)
                time.sleep(TimeOut.CONNECTION_CLOSED * TimeOut.ONE_MINUTE)

            except Exception as ex:
                logger.error(f"Unexpected error: {ex}\n")

    def stop_recording(self):
        self.stop_flag = True
        logger.info(f"Stop flag set for {self.user}")
        
        # Detener sampling en tiempo real
        if hasattr(self, 'video_sampler'):
            self.video_sampler.stop_sampling()
        
        # CRÍTICO: Cerrar cliente HTTP de TikTok para evitar conexiones CLOSE_WAIT
        if hasattr(self, 'tiktok') and self.tiktok:
            try:
                self.tiktok.close()
                logger.debug(f"TikTok HTTP client closed for {self.user} during stop")
            except Exception as e:
                logger.warning(f"Error closing TikTok HTTP client for {self.user} during stop: {e}")
    
    def check_file_size(self, filepath):
        """Check if file size exceeds the maximum allowed size"""
        try:
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                return file_size >= self.max_file_size
            return False
        except Exception as e:
            logger.error(f"Error checking file size: {e}")
            return False
    def _check_total_recording_parts_size(self, current_filepath):
        """Check if total size of all recording parts plus current file exceeds the maximum allowed size"""
        try:
            total_size = 0
            valid_parts = 0
            
            # Sumar tamaño de todas las partes válidas previas
            for part_file in self.recording_parts:
                if os.path.exists(part_file):
                    part_size = os.path.getsize(part_file)
                    
                    # Para archivos .mkv (grabación en progreso), asumir válidos si tienen tamaño razonable
                    if part_file.endswith('.mkv'):
                        if part_size > 1024:  # > 1KB es válido para .mkv
                            total_size += part_size
                            valid_parts += 1
                            logger.debug(f"Valid recording part (MKV) {valid_parts}: {os.path.basename(part_file)} ({part_size/1024/1024:.1f}MB)")
                    else:
                        # Para archivos .mp4, usar validación de frames solo si es necesario
                        # Para cálculo de fragmentación, asumir válidos si tienen tamaño razonable
                        if part_size > 10240:  # > 10KB es válido para .mp4
                            total_size += part_size
                            valid_parts += 1
                            logger.debug(f"Valid recording part (MP4) {valid_parts}: {os.path.basename(part_file)} ({part_size/1024/1024:.1f}MB)")
            
            # Sumar tamaño del archivo actual en progreso
            current_size = 0
            if current_filepath and os.path.exists(current_filepath):
                current_size = os.path.getsize(current_filepath)
                total_size += current_size
                logger.debug(f"Current recording file: {os.path.basename(current_filepath)} ({current_size/1024/1024:.1f}MB)")
            
            if valid_parts > 0 or current_size > 0:
                size_mb = total_size / (1024 * 1024)
                max_size_mb = self.max_file_size / (1024 * 1024)
                
                # Añadir un margen de seguridad configurable para la concatenación
                safety_margin_percent = config.fragmentation_safety_margin_percent / 100.0
                safety_margin = self.max_file_size * safety_margin_percent
                threshold_reached = total_size >= (self.max_file_size - safety_margin)
                
                current_size_mb = current_size / (1024 * 1024)
                safety_margin_mb = safety_margin / (1024 * 1024)
                
                if threshold_reached:
                    logger.warning(f"🚨 FRAGMENTATION THRESHOLD REACHED: Total={size_mb:.1f}MB (Current={current_size_mb:.1f}MB + {valid_parts} parts) | Limit={max_size_mb:.1f}MB | Safety={safety_margin_mb:.1f}MB")
                else:
                    logger.info(f"📊 SIZE CHECK: Total={size_mb:.1f}MB (Current={current_size_mb:.1f}MB + {valid_parts} parts) | Limit={max_size_mb:.1f}MB | Safety={safety_margin_mb:.1f}MB")
                
                return threshold_reached
            
            return False
        except Exception as e:
            logger.error(f"Error checking total recording parts size: {e}")
            return False
    
    def should_fragment_recording(self, filepath, segment_files=None):
        """Check if recording should be fragmented based on file size and time interval"""
        current_time = time.time()
        if current_time - self.last_size_check >= self.size_check_interval:
            self.last_size_check = current_time
            
            # Para grabación continua con FFmpeg directo, verificar todas las partes válidas
            if config.enable_ffmpeg_direct_recording and self.recording_parts:
                if self._check_total_recording_parts_size(filepath):
                    size_gb = config.max_file_size_bytes / (1024 * 1024 * 1024)
                    logger.info(f"Total recording parts size exceeded {size_gb:.1f}GB. Fragmenting recording. Current fragment: {self.fragment_number}")
                    return True
            else:
                # Método original para grabación sin segmentación
                if self.check_file_size(filepath):
                    size_gb = config.max_file_size_bytes / (1024 * 1024 * 1024)
                    logger.info(f"File size exceeded {size_gb:.1f}GB. Fragmenting recording. Current fragment: {self.fragment_number}")
                    return True
        return False

    def start_recording(self):
        # Usar URL cached para la grabación inicial
        try:
            live_url = self._get_stream_url_cached()
            if not live_url:
                raise LiveNotFound(TikTokError.RETRIEVE_LIVE_URL)
        except TikTokLiveRateLimitException:
            raise
        
        # Log del inicio de la descarga (una sola vez)
        logger.info(f"Starting stream download from: {live_url}")
        
        # Ejecutar callback de inicio exitoso si está definido
        if hasattr(self, 'on_recording_started_callback') and self.on_recording_started_callback:
            try:
                self.on_recording_started_callback()
            except Exception as callback_error:
                logger.warning(f"Error in recording started callback: {callback_error}")

        current_date = time.strftime("%Y.%m.%d_%H-%M-%S", time.localtime())

        if isinstance(self.output, str) and self.output != '':
            if not (self.output.endswith('/') or self.output.endswith('\\')):
                if os.name == 'nt':
                    self.output = self.output + "\\"
                else:
                    self.output = self.output + "/"

        # Crear estructura de carpetas organizadas
        organized_output_path = self._create_organized_output_path(current_date)
        
        # Generar nombres según el patrón requerido: _cont1_part2, _cont2_part2, etc.
        if self.fragment_number > 1:
            fragment_suffix = f"_cont{self.current_part_number}_part{self.fragment_number}"
        else:
            fragment_suffix = f"_cont{self.current_part_number}" if self.current_part_number > 1 else ""
        output = f"{organized_output_path}TK_{self.user}_{current_date}{fragment_suffix}_flv.mp4"

        if self.duration:
            logger.info(f"Started recording for {self.duration} seconds ")
        else:
            logger.info("Started recording...")

        logger.info("[Recording can be stopped via Telegram bot]")
        
        # Decidir método de grabación
        if config.enable_ffmpeg_direct_recording:
            self._start_ffmpeg_direct_recording(live_url, output)
        else:
            logger.info("Using CLASSIC single-file recording method")
            self._start_classic_recording(live_url, output)

    def _start_classic_recording(self, live_url, output):
        """Método de grabación clásico original - una sola pieza"""
        buffer_size = config.buffer_size_bytes
        buffer = bytearray()
        
        # Reiniciar flag de stop
        self.stop_flag = False
        
        logger.info(f"Starting classic recording: {output}")
        
        with open(output, "wb") as out_file:
            try:
                for chunk in self.tiktok.download_live_stream(live_url):
                    # Verificar flag de stop
                    if self.stop_flag:
                        logger.info("Recording stopped via bot command.")
                        break

                    if not self.tiktok.is_room_alive(self.room_id):
                        logger.info("User is no longer live. Stopping recording.")
                        break
                    
                    # Check if we need to fragment due to file size
                    if self.should_fragment_recording(output):
                        logger.info("File size limit reached, fragmenting...")
                        break

                    if chunk:
                        # Escribir chunk directamente al buffer
                        buffer.extend(chunk)
                        if len(buffer) >= buffer_size:
                            out_file.write(buffer)
                            buffer.clear()
                            
                    if self.duration and time.time() - time.time() >= self.duration:
                        logger.info("Duration limit reached.")
                        break
            
            except Exception as ex:
                logger.error(f"Error in classic recording: {ex}")
            
            finally:
                # Forzar flush del buffer final
                if buffer:
                    out_file.write(buffer)
                    buffer.clear()
                out_file.flush()
                try:
                    os.fsync(out_file.fileno())
                except Exception as fsync_error:
                    logger.warning(f"Could not fsync file: {fsync_error}")
        
        # Convertir a MP4 y procesar para Telegram
        logger.info(f"Classic recording finished: {output}")
        VideoManagement.convert_flv_to_mp4(output)
        mp4_output = output.replace('_flv.mp4', '.mp4')
        
        self._process_final_output(mp4_output)

    def _process_final_output(self, mp4_path, is_fragmentation=False, is_continuous=False):
        """
        Procesa el archivo final MP4 para Telegram (collage, thumbnail, upload)
        
        Args:
            mp4_path: Ruta al archivo MP4 final
            is_fragmentation: True si está siendo procesado por fragmentación por tamaño
            is_continuous: True si es video concatenado de grabación continua (sin mostrar partes)
        """
        if not self.use_telegram:
            logger.info(f"Telegram upload disabled, recording saved: {mp4_path}")
            return
        
        if not os.path.exists(mp4_path):
            logger.error(f"Final MP4 file not found: {mp4_path}")
            return
        
        # Validar que el archivo MP4 no esté corrupto
        if not self._validate_mp4_file(mp4_path):
            logger.error(f"MP4 file is corrupted, skipping upload: {mp4_path}")
            return
        
        # Generar collage de screenshots y thumbnail si está habilitado
        collage_path = None
        thumbnail_path = None
        if config.enable_video_collage:
            collage_path = self._generate_video_collage(mp4_path)
            thumbnail_path = self._generate_video_thumbnail(mp4_path)
        
        # Determinar si es una grabación fragmentada
        # Una grabación es fragmentada si:
        # 1. Es parte 2 o superior (fragmentación ya ocurrió)
        # 2. Está siendo procesada por fragmentación por tamaño (incluye Parte 1)
        # NOTA: Videos concatenados de grabación continua NO son fragmentados (is_continuous=True)
        if is_continuous:
            is_fragmented = False  # Video concatenado de grabación continua - no mostrar partes
        else:
            is_fragmented = (self.fragment_number > 1) or is_fragmentation
        
        # MARCAR EN REDIS como procesado (para grabaciones directas sin cola)
        self._mark_recording_processed_redis()
        
        # Usar el upload manager para subir a Telegram
        try:
            asyncio.run(self.upload_manager.add_upload_task(
                mp4_path,
                self.telegram_chat_id,
                self.user,
                self.fragment_number,
                is_fragmented,
                collage_path=collage_path,
                thumbnail_path=thumbnail_path
            ))
            logger.info(f"Upload task added for {self.user}: {mp4_path}")
        except Exception as e:
            logger.error(f"Error adding upload task for {self.user}: {e}")
            # MARCAR EN REDIS como fallido si falla el upload
            self._mark_recording_failed_redis(f"Upload task failed: {e}")

    def _start_ffmpeg_direct_recording(self, live_url, output):
        """
        Método de grabación directo usando FFmpeg - Ultra eficiente
        FFmpeg maneja todo: descarga, reconexiones, y graba directo a MP4
        """
        
        # Reiniciar flag de stop
        self.stop_flag = False
        
        # Generar output MKV temporal y MP4 final
        mp4_output = output.replace('_flv.mp4', '.mp4')
        mkv_output = mp4_output.replace('.mp4', '.mkv')  # Grabar en .mkv temporal
        
        # Reset corruption detection flags
        self.corruption_detected = False
        self.avcc_error_detected = False  # Reset flag AVCC
        self.corruption_count = 0
        self.ffmpeg_stderr_log = []
        
        # Crear archivo de log para stderr de FFmpeg solo si está habilitado
        if config.enable_ffmpeg_logs:
            # Usar carpeta base (sin subcarpetas de usuario) para facilitar acceso
            base_output = self.output if self.output else config.output_directory
            if not base_output.endswith(('/', '\\')):
                base_output += os.sep
            
            # Crear carpeta de logs si no existe
            logs_dir = os.path.join(base_output, "ffmpeg_logs")
            os.makedirs(logs_dir, exist_ok=True)
            
            # Generar nombre de archivo con fecha legible
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            log_filename = f"ffmpeg_stderr_{self.user}_{timestamp}.log"
            self.ffmpeg_stderr_log_path = os.path.join(logs_dir, log_filename)
            
            # Crear y abrir archivo de log para escritura en tiempo real
            try:
                self.ffmpeg_log_file = open(self.ffmpeg_stderr_log_path, 'w', encoding='utf-8', buffering=1)  # Line buffered
                # Escribir header del log
                self.ffmpeg_log_file.write(f"FFmpeg stderr log for user: {self.user}\n")
                self.ffmpeg_log_file.write(f"Recording session started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                self.ffmpeg_log_file.write(f"FFmpeg loglevel: info (captures all output)\n")
                self.ffmpeg_log_file.write(f"Monitoring for corruption patterns in real-time...\n")
                self.ffmpeg_log_file.write(f"Note: ALL stderr output from FFmpeg will be written here\n")
                self.ffmpeg_log_file.write("=" * 80 + "\n\n")
                self.ffmpeg_log_file.flush()
                logger.info(f"🔍 FFmpeg stderr logging enabled - logs will be saved to: {self.ffmpeg_stderr_log_path}")
            except Exception as e:
                logger.error(f"Could not create FFmpeg log file: {e}")
                self.ffmpeg_log_file = None
        else:
            self.ffmpeg_log_file = None
            self.ffmpeg_stderr_log_path = None
            logger.info(f"🔍 FFmpeg stderr logging disabled via configuration")
        
        logger.info(f"Starting FFmpeg robust recording: {mkv_output}")
        logger.info(f"Stream URL: {live_url}")
        
        # Construir comando FFmpeg optimizado para TikTok streams (grabación en .mkv)
        ffmpeg_cmd = [
             'ffmpeg',
            '-y',  # Sobrescribir archivo de salida
            '-loglevel', config.ffmpeg_loglevel,  # Reducir logs verbosos
            '-stats',  # Forzar mostrar estadísticas completas (frame, fps, etc.) para compatibilidad Linux
            '-err_detect', 'bitstream',

            # Configuraciones de entrada para streams FLV de TikTok
            '-reconnect', '1',  # Habilitar reconexión automática
            '-reconnect_streamed', '1',  # Reconectar streams
            '-reconnect_delay_max', str(config.ffmpeg_reconnect_delay_max),  # Max delay entre reconexiones
            '-rw_timeout', str(config.ffmpeg_timeout * 1000000),  # Read/write timeout en microsegundos
            '-user_agent', config.ffmpeg_user_agent,  # User agent para TikTok

            # Configuraciones optimizadas de buffering (reducir RAM sin afectar calidad)
            '-fflags', '+nobuffer+discardcorrupt',
            '-rtbufsize', '64M',         # Buffer optimizado para TikTok streams (era 300M)
            '-probesize', '10M',         # Análisis eficiente del stream (era 50M)
            '-analyzeduration', '2M',    # Duración de análisis optimizada (era 10M)
            #'-max_muxing_queue_size', '1024',  # Limitar cola de muxing
            '-thread_queue_size', '512',       # Limitar cola de threads
            #'-bufsize', '32M',                 # Buffer de salida controlado
            # URL de entrada
            '-i', live_url,
        ]
        
        # Agregar skip inicial si está habilitado para estabilizar la conexión
        if config.enable_initial_skip and config.initial_skip_seconds > 0:
            ffmpeg_cmd.extend(['-ss', str(config.initial_skip_seconds)])
            logger.info(f"⏭️ Initial skip enabled: skipping first {config.initial_skip_seconds}s for stream stabilization")
        
        # Continuar con configuraciones de salida
        ffmpeg_cmd.extend([
            # Configuraciones de salida para grabación robusta en .mkv
            '-c:v', 'copy',  # Copy video stream sin recodificar
            '-c:a', 'copy',  # Copy audio stream sin recodificar
            # Parámetros de robustez para MKV con escritura inmediata (reducir RAM)
            '-max_interleave_delta', '0',  # Mejor manejo de streams desincronizados
            '-flush_packets', '1',  # CRÍTICO: Forzar escritura de buffers
            '-max_delay', '500000',  # Máximo 0.5s de delay para evitar buffering excesivo en RAM
            '-avoid_negative_ts', 'disabled',  # No esperar a corregir timestamps (escribir inmediatamente)
            '-fflags', '+nobuffer+discardcorrupt+flush_packets+genpts+igndts',  # Escritura agresiva + regenerar timestamps
            '-f', 'matroska',  # Formato Matroska (MKV) para máxima flexibilidad
            
            # Archivo de salida temporal (.mkv)
            mkv_output
        ])
        
        # Variables para monitoreo
        ffmpeg_process = None
        recording_successful = False
        start_time = time.time()
        last_progress_log = time.time()
        fragmentation_triggered = False
        
        try:
            logger.info(f"Executing FFmpeg command for {self.user}")
            logger.debug(f"FFmpeg command: {' '.join(ffmpeg_cmd)}")
            
            # Iniciar proceso FFmpeg con aislamiento de señales y acceso a stdin
            # CRÍTICO: Separar stdout y stderr para monitoreo de corrupción
            if os.name == 'nt':  # Windows
                # En Windows, crear un nuevo grupo de procesos para aislar FFmpeg
                ffmpeg_process = subprocess.Popen(
                    ffmpeg_cmd,
                    stdin=subprocess.PIPE,      # CRÍTICO: Habilitar stdin para comando 'q'
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,     # CAMBIO: Separar stderr para monitoreo
                    universal_newlines=True,    # CAMBIO: Usar texto para lectura de logs
                    bufsize=1,                  # Line buffered para lectura en tiempo real
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                )
            else:  # Linux/Mac
                # En Unix, usar un nuevo session ID
                ffmpeg_process = subprocess.Popen(
                    ffmpeg_cmd,
                    stdin=subprocess.PIPE,      # CRÍTICO: Habilitar stdin para comando 'q'
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,     # CAMBIO: Separar stderr para monitoreo
                    universal_newlines=True,    # CAMBIO: Usar texto para lectura de logs
                    bufsize=1,                  # Line buffered para lectura en tiempo real
                    preexec_fn=os.setsid
                )
            
            # Iniciar hilo de monitoreo de stderr para detectar corrupción
            self.corruption_monitor_thread = threading.Thread(
                target=self._monitor_ffmpeg_stderr,
                args=(ffmpeg_process,),
                daemon=True
            )
            self.corruption_monitor_thread.start()
            
            logger.debug(f"FFmpeg process started with PID {ffmpeg_process.pid} for {self.user}")
            
            # Guardar referencia al proceso FFmpeg para el callback del sampler
            self.ffmpeg_process = ffmpeg_process
            
            # Iniciar guardian de memoria FFmpeg
            if config.enable_ffmpeg_memory_guardian and psutil:
                self.memory_monitor_thread = threading.Thread(
                    target=self._monitor_ffmpeg_memory,
                    args=(ffmpeg_process.pid,),
                    daemon=True,
                    name=f"MemoryGuardian-{self.user}"
                )
                self.memory_monitor_thread.start()
                logger.debug(f"🔍 Memory guardian thread started for {self.user}")
            
            # Iniciar sampling en tiempo real del archivo de grabación
            self.video_sampler.start_sampling(mkv_output)
            
            # Monitorear el proceso mientras graba
            while ffmpeg_process.poll() is None:
                # Verificar flag de stop
                if self.stop_flag:
                    logger.info(f"Stop flag detected, terminating FFmpeg cleanly for {self.user}")
                    self._terminate_ffmpeg_cleanly(ffmpeg_process, "user stop request")
                    break
                
                # NUEVO: Verificar si se detectó corrupción (incluye memory restart)
                if self.corruption_detected:
                    logger.warning(f"🚨 CORRUPTION DETECTED for {self.user}, stopping current recording to restart")
                    self._terminate_ffmpeg_cleanly(ffmpeg_process, "corruption detected")
                    fragmentation_triggered = True  # Tratar como fragmentación para reiniciar
                    break
                
                # Verificar si el usuario sigue en vivo periódicamente
                current_time = time.time()
                if current_time - last_progress_log >= 30:  # Cada 30 segundos
                    if not self.tiktok.is_room_alive(self.room_id):
                        logger.info(f"User {self.user} is no longer live, stopping FFmpeg recording cleanly")
                        self._terminate_ffmpeg_cleanly(ffmpeg_process, "user no longer live")
                        break
                    
                    # Verificar si necesitamos fragmentar por tamaño total
                    # Usar el archivo .mkv actual en lugar del .mp4 que no existe aún
                    if self.should_fragment_recording(mkv_output):
                        logger.info(f"Size limit reached during recording, fragmenting for {self.user}")
                        fragmentation_triggered = True
                        self._terminate_ffmpeg_cleanly(ffmpeg_process, "size limit reached")
                        break
                    
                    # Log de progreso
                    elapsed_minutes = (current_time - start_time) / 60
                    logger.info(f"FFmpeg recording in progress for {self.user}: {elapsed_minutes:.1f} minutes")
                    last_progress_log = current_time
                
                # Verificar duración límite si está configurada
                if self.duration and (current_time - start_time) >= self.duration:
                    logger.info(f"Duration limit reached ({self.duration}s), stopping FFmpeg recording for {self.user}")
                    self._terminate_ffmpeg_cleanly(ffmpeg_process, "duration limit reached")
                    break
                
                # Pausa pequeña para no sobrecargar CPU
                time.sleep(1)
            
            # Obtener código de salida
            return_code = ffmpeg_process.wait()
            
            if return_code == 0:
                logger.info(f"FFmpeg recording completed successfully for {self.user}")
                recording_successful = True
            elif self.stop_flag:
                logger.info(f"FFmpeg recording stopped by user request for {self.user}")
                recording_successful = True  # Parada manual es éxito
            else:
                logger.error(f"FFmpeg recording failed with return code {return_code} for {self.user}")
                
                # Leer stderr para más información
                if ffmpeg_process.stdout:
                    error_output = ffmpeg_process.stdout.read()
                    if error_output:
                        logger.error(f"FFmpeg error output: {error_output}")
        
        except Exception as e:
            logger.error(f"Error during FFmpeg recording for {self.user}: {e}")
            
            # Limpiar proceso si está corriendo
            if ffmpeg_process and ffmpeg_process.poll() is None:
                try:
                    ffmpeg_process.terminate()
                    ffmpeg_process.wait(timeout=10)
                except:
                    try:
                        ffmpeg_process.kill()
                    except:
                        pass
        
        finally:
            # Asegurar que el proceso esté terminado
            if ffmpeg_process and ffmpeg_process.poll() is None:
                try:
                    ffmpeg_process.kill()
                except:
                    pass
            
            # CRÍTICO: Cerrar explícitamente todas las pipes para evitar file descriptor leaks
            if ffmpeg_process:
                try:
                    if ffmpeg_process.stdin and not ffmpeg_process.stdin.closed:
                        ffmpeg_process.stdin.close()
                except:
                    pass
                try:
                    if ffmpeg_process.stdout and not ffmpeg_process.stdout.closed:
                        ffmpeg_process.stdout.close()
                except:
                    pass
                try:
                    if ffmpeg_process.stderr and not ffmpeg_process.stderr.closed:
                        ffmpeg_process.stderr.close()
                except:
                    pass
            
            # Esperar a que termine el hilo de monitoreo
            if self.corruption_monitor_thread and self.corruption_monitor_thread.is_alive():
                self.corruption_monitor_thread.join(timeout=5)
            
            # Asegurar que el archivo de log se cierre si aún está abierto
            if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file and not self.ffmpeg_log_file.closed:
                try:
                    self.ffmpeg_log_file.write(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Recording session ended\n")
                    self.ffmpeg_log_file.close()
                except Exception as e:
                    logger.warning(f"Error closing FFmpeg log file in cleanup: {e}")
            
            
            # Generar resumen final del log
            self._save_ffmpeg_stderr_log()
        
        # Verificar que el archivo .mkv se creó correctamente
        if os.path.exists(mkv_output) and os.path.getsize(mkv_output) > 0:
            file_size_mb = os.path.getsize(mkv_output) / (1024 * 1024)
            logger.info(f"FFmpeg recording output (.mkv): {mkv_output} ({file_size_mb:.1f}MB)")
            
            
            recording_successful = True
        else:
            logger.error(f"FFmpeg recording failed - no valid .mkv output file for {self.user}")
            recording_successful = False
        
        # CRÍTICO: Manejar URL expiration ANTES de procesar la parte
        if self.url_expiration_detected and not self.stop_flag:
            logger.warning(f"🔄 URL EXPIRATION DETECTED: Attempting stream continuation for {self.user}")
            if self._validate_stream_continuation():
                logger.info(f"✅ Stream continues after URL expiration for {self.user}, starting continuation...")
                self._start_continuous_recording_part()
                return
            else:
                logger.info(f"❌ Stream ended after URL expiration for {self.user}, processing final recording...")
        
        # CRÍTICO: Manejar corrupción de timestamp matroska ANTES de añadir a recording_parts
        if self.matroska_corruption_detected and os.path.exists(mkv_output):
            logger.error(f"🗑️ Deleting matroska corrupted segment: {os.path.basename(mkv_output)}")
            try:
                # Normalize path for Windows compatibility
                normalized_path = os.path.normpath(mkv_output)
                file_size = os.path.getsize(normalized_path) / (1024 * 1024)  # MB
                os.remove(normalized_path)
                logger.info(f"✅ Matroska corrupted segment deleted: {os.path.basename(mkv_output)} ({file_size:.2f}MB)")
                # Marcar como no exitoso para que no se añada a recording_parts
                recording_successful = False
            except Exception as e:
                logger.error(f"Error deleting matroska corrupted segment: {e}")
        
        # CRÍTICO: Siempre añadir partes válidas a la lista, incluso si la actual falla
        if recording_successful and os.path.exists(mkv_output):
            # GAP TRIMMING: Cortar gap detectado ANTES de añadir a recording_parts
            if self.gap_detected and self.gap_start_timestamp is not None:
                logger.info(f"Gap detected, trimming segment before adding to parts: {mkv_output}")
                trimmed_output = self._trim_gap_from_segment(mkv_output, self.gap_start_timestamp)
                if trimmed_output:
                    mkv_output = trimmed_output  # Usar archivo trimmed
                    logger.info(f"Gap trimming successful: {mkv_output}")
                else:
                    logger.warning(f"Gap trimming failed, using original file: {mkv_output}")
                
                # Reset gap detection flags
                self.gap_detected = False
                self.gap_start_timestamp = None
            
            # Añadir esta parte (.mkv) a la lista de partes de la sesión
            self.recording_parts.append(mkv_output)
            logger.info(f"Added recording part {len(self.recording_parts)} for {self.user}: {mkv_output}")
            
            # NUEVO: Guardar estado persistente después de agregar parte
            self._save_session_state()
            
            # MANEJO DE FRAGMENTACIÓN POR TAMAÑO (no corrupción)
            # Solo fragmentar si fue detectado DURANTE la grabación, no al final
            if not self.corruption_detected and fragmentation_triggered:
                logger.info(f"File size limit reached, fragmenting recording for {self.user}")
                
                # Procesar todos los segmentos acumulados hasta ahora usando la cola de post-procesamiento
                if len(self.recording_parts) >= config.min_segments_for_queue:
                    logger.info(f"Sending {len(self.recording_parts)} segments to post-processing queue for fragmentation")
                    asyncio.run(self._submit_to_postprocessing_queue(is_fragmentation=True))
                else:
                    # Si hay pocos segmentos, procesar inmediatamente
                    logger.info(f"Processing {len(self.recording_parts)} segments immediately (below queue threshold)")
                    self._process_all_parts_immediately(is_fragmentation=True)
                
                # Limpiar la lista de parts después del procesamiento para el próximo fragmento
                self.recording_parts = []
                logger.debug(f"Cleared recording_parts for next fragment cycle for {self.user}")
                
                if self.auto_restart_callback and not self.stop_flag:
                    logger.info(f"Auto-restarting FFmpeg recording for {self.user} - Fragment {self.fragment_number + 1}")
                    self.auto_restart_callback(self.fragment_number + 1)
                return
            
            # NUEVO: MANEJO DE FRAGMENTACIÓN POR CORRUPCIÓN DETECTADA
            if self.corruption_detected and not self.stop_flag:
                logger.warning(f"🚨 CORRUPTION RESTART: Processing current part and restarting with contX for {self.user}")
                
                # AVCC error detected - pero el post-procesamiento se hará en la cola
                if self.avcc_error_detected:
                    logger.warning(f"🩹 AVCC error detected for {self.user} - will be handled by post-processing queue")
                
                # CRÍTICO: Verificar stop flag antes de reiniciar
                if self.stop_flag:
                    logger.info(f"Stop flag detected, not restarting after corruption for {self.user}")
                    logger.info(f"Processing existing parts before terminating for {self.user}")
                    # Continuar al procesamiento final en lugar de return directo
                    # Esto permite procesar las partes ya grabadas
                    
                # Solo continuar si el usuario sigue en vivo Y no hay stop flag
                elif self._validate_stream_continuation():
                    logger.info(f"✅ User {self.user} still live, attempting restart with retry logic...")
                    
                    max_attempts = config.max_restart_attempts_after_corruption
                    
                    for attempt in range(1, max_attempts + 1):
                        logger.info(f"🔄 Restart attempt {attempt}/{max_attempts} for {self.user}")
                        
                        # Para corruption restart, usar cached URL primero (más eficiente)
                        if attempt == 1 and self._is_cached_url_valid():
                            logger.info(f"🔄 Using cached stream URL for corruption restart: {self.user}")
                            # No invalidar cache, usar URL existente
                        else:
                            # Si primer intento falló o cache inválido, entonces fetch fresh URL
                            logger.info(f"🔄 Fetching fresh stream URL for restart attempt {attempt}: {self.user}")
                            fresh_url = self._force_fresh_stream_url()
                            if not fresh_url:
                                logger.warning(f"⚠️ Could not obtain fresh URL on attempt {attempt} for {self.user}")
                                # Continuar con el intento usando URL existente si hay
                        
                        # Intentar reinicio
                        restart_success = self._attempt_corruption_restart()
                        
                        if restart_success:
                            logger.info(f"✅ Restart successful on attempt {attempt} for {self.user}")
                            return  # Éxito, continuar grabación
                        else:
                            logger.warning(f"❌ Restart attempt {attempt} failed for {self.user}")
                            
                            if attempt < max_attempts:
                                delay = config.restart_retry_delay_seconds
                                logger.info(f"⏳ Waiting {delay}s before next restart attempt...")
                                time.sleep(delay)
                    
                    # Todos los intentos fallaron
                    logger.error(f"❌ All {max_attempts} restart attempts failed for {self.user}, terminating recording")
                    # Continuar con procesamiento final
                else:
                    logger.info(f"❌ User {self.user} no longer live, ending recording after corruption")
                    # Continuar con procesamiento final
            
            # Si FFmpeg terminó inesperadamente (con o sin error) y la grabación continua está habilitada
            # CRÍTICO: Incluir códigos de error por corrupción para reiniciar automáticamente
            logger.info(f"🔍 Checking continuation conditions for {self.user}:")
            logger.info(f"   stop_flag: {self.stop_flag}")
            logger.info(f"   enable_continuous_recording: {config.enable_continuous_recording}")
            logger.info(f"   current_part_number: {self.current_part_number} < max: {config.max_recording_parts}")
            
            if (not self.stop_flag and 
                config.enable_continuous_recording and 
                self.current_part_number < config.max_recording_parts):
                
                # Determinar si debe continuar basado en el código de retorno y detección de corrupción
                should_continue = False
                
                if return_code == 0:
                    # FFmpeg terminó limpiamente
                    logger.info(f"FFmpeg terminated normally for {self.user}, checking if stream continues...")
                    should_continue = True
                elif self.corruption_detected and return_code != 0:
                    # FFmpeg terminó con error debido a corrupción detectada
                    logger.info(f"FFmpeg terminated with error code {return_code} due to corruption for {self.user}, checking if stream continues...")
                    should_continue = True
                elif self.url_expiration_detected:
                    # FFmpeg terminó con error de URL (404, 403, etc.) - error temporal recuperable
                    logger.warning(f"🔄 FFmpeg terminated with URL error (url_expiration_detected=True) for {self.user}, checking if user still live...")
                    should_continue = True
                else:
                    logger.info(f"FFmpeg terminated with error code {return_code} for {self.user}, not continuing...")
                
                if should_continue:
                    # Validar si el usuario sigue en vivo
                    if self._validate_stream_continuation():
                        logger.info(f"Stream continues for {self.user}, starting automatic continuation...")
                        self._start_continuous_recording_part()
                        return
                    else:
                        logger.info(f"Stream ended for {self.user}, processing final recording...")
        
        # CRÍTICO: Procesar partes existentes SIEMPRE, incluso si la grabación actual falló
        # Esto evita perder partes válidas cuando la última parte falla
        if len(self.recording_parts) > 0:
            logger.info(f"Processing {len(self.recording_parts)} existing recording parts for {self.user} (current recording success: {recording_successful})")
            
            # PRIMERO: Convertir todos los archivos .mkv a .mp4
            converted_parts = []
            for i, mkv_part in enumerate(self.recording_parts):
                if mkv_part.endswith('.mkv'):
                    mp4_part = mkv_part.replace('.mkv', '.mp4')
                    logger.info(f"Converting part {i+1}/{len(self.recording_parts)}: {mkv_part} -> {mp4_part}")
                    if self._convert_mkv_to_mp4(mkv_part, mp4_part):
                        converted_parts.append(mp4_part)
                        # Limpiar archivo .mkv temporal
                        try:
                            # Normalize path for Windows compatibility
                            normalized_path = os.path.normpath(mkv_part)
                            os.remove(normalized_path)
                            logger.debug(f"Cleaned up temporary MKV file: {mkv_part}")
                        except Exception as cleanup_error:
                            logger.warning(f"Could not cleanup MKV file {mkv_part}: {cleanup_error}")
                    else:
                        logger.error(f"Failed to convert {mkv_part} to MP4, skipping this part")
                else:
                    # Ya es MP4, añadir directamente
                    converted_parts.append(mkv_part)
            
            # Actualizar recording_parts con los archivos MP4 convertidos
            self.recording_parts = converted_parts
            logger.info(f"Converted all parts to MP4. Final parts count: {len(self.recording_parts)}")
            
            if len(self.recording_parts) > 0:
                # Usar la cola de post-procesamiento asíncrona
                asyncio.run(self._submit_to_postprocessing_queue(is_fragmentation=False))
            else:
                logger.warning(f"No valid recording parts available for {self.user}")
        else:
            logger.warning(f"No recording parts to process for {self.user} - recording may have failed completely")
        
        # NUEVO: Limpiar archivo de estado de la sesión
        self._cleanup_session_state_file()
        
        # Cleanup final
        if self.cleanup_callback:
            logger.info(f"FFmpeg recording finished definitively for {self.user}, calling cleanup")
            self.cleanup_callback()

    def _convert_mkv_to_mp4(self, mkv_file: str, mp4_file: str) -> bool:
        """
        Convierte archivo .mkv a .mp4 con optimizaciones para streaming
        
        Args:
            mkv_file: Ruta al archivo .mkv de entrada
            mp4_file: Ruta al archivo .mp4 de salida
            
        Returns:
            bool: True si la conversión fue exitosa, False en caso contrario
        """
        try:
            # VALIDACIÓN CRÍTICA: Verificar que el archivo MKV tenga peso > 1KB antes de conversión
            if not os.path.exists(mkv_file):
                logger.error(f"MKV file does not exist: {mkv_file}")
                return False
            
            mkv_size = os.path.getsize(mkv_file)
            if mkv_size <= 1024:  # <= 1KB
                logger.warning(f"MKV file too small ({mkv_size} bytes), deleting: {mkv_file}")
                try:
                    # Normalize path for Windows compatibility
                    normalized_path = os.path.normpath(mkv_file)
                    os.remove(normalized_path)
                    logger.info(f"Successfully deleted undersized MKV file: {mkv_file}")
                except Exception as e:
                    logger.error(f"Failed to delete undersized MKV file {mkv_file}: {e}")
                return False
            
            logger.info(f"Converting MKV to MP4: {mkv_file} -> {mp4_file} (source: {mkv_size/1024/1024:.1f}MB)")
            
            # Verificación condicional de streams para optimizar CPU
            if config.enable_ffprobe_verification:
                try:
                    probe_result = subprocess.run([
                        'ffprobe', '-v', 'quiet', 
                        '-show_entries', 'stream=codec_type',
                        '-of', 'csv=p=0', mkv_file
                    ], capture_output=True, text=True, timeout=10)
                    
                    if probe_result.returncode != 0:
                        logger.warning(f"Could not verify MKV file streams: {mkv_file}")
                        
                except Exception as e:
                    logger.warning(f"Stream verification failed for {mkv_file}: {e}")
            else:
                logger.debug(f"FFprobe verification disabled for performance - skipping MKV analysis")
            
            convert_cmd = [
                'ffmpeg',
                '-y',  # Sobrescribir archivo de salida
                '-loglevel', config.ffmpeg_loglevel,
                
                # Enhanced input processing for timing reconstruction (MKV-optimized)
                '-fflags', '+genpts+discardcorrupt+igndts+flush_packets',  # Complete timestamp regeneration
                '-err_detect', 'ignore_err',          # Ignore minor decoding errors
                '-analyzeduration', '10000000',       # Deep analysis for timing patterns
                '-probesize', '10000000',             # Large probe buffer
                
                '-i', mkv_file,  # Input MKV file
                
                # For MKV: No need for AAC bitstream conversion like TS
                # STREAM COPY (NO re-encoding) with iOS-specific fixes
                '-c:v', 'copy',                       # Copy video (NO re-encoding)
                '-c:a', 'copy',                       # Copy audio (NO re-encoding)
                
                # CRITICAL: Frame rate and timing fixes for iOS (addresses 30fps→29.88fps issue)
                '-r', '30',                           # Force constant 30fps (prevent drift)
                '-fps_mode', 'cfr',                   # Force CFR mode
                
                # Perfect timestamp reconstruction for iOS/QuickTime
                '-avoid_negative_ts', 'make_zero',    # No negative timestamps
                '-start_at_zero',                     # Force start at timestamp 0
                '-max_interleave_delta', '0',         # Perfect interleaving
                '-max_muxing_queue_size', '4096',     # Large buffer for stability
                
                # iOS-optimized MP4 container (critical for QuickTime compatibility)
                '-movflags', '+faststart+use_metadata_tags',  # iOS streaming optimization
                '-brand', 'mp42',                     # iOS-compatible brand
                
                # Compatibility enhancements for iOS/QuickTime
                '-strict', '-2',                      # Allow experimental features for better compatibility
                '-map_metadata', '0',                 # Preserve metadata
                
                '-f', 'mp4',
                mp4_file
            ]
                
            logger.debug(f"MKV->MP4 conversion command: {' '.join(convert_cmd)}")
            
            # Calcular timeout dinámico basado en tamaño del archivo
            base_timeout = config.mkv_to_mp4_timeout_seconds
            
            # Timeout dinámico: base + tiempo adicional basado en tamaño del archivo
            if os.path.exists(mkv_file):
                file_size_gb = os.path.getsize(mkv_file) / (1024 * 1024 * 1024)
                additional_timeout = int(file_size_gb * config.mkv_to_mp4_timeout_per_gb_seconds)
                conversion_timeout = base_timeout + additional_timeout
                logger.info(f"MKV file size: {file_size_gb:.2f}GB, conversion timeout: {conversion_timeout}s ({base_timeout}s base + {additional_timeout}s for size)")
            else:
                conversion_timeout = base_timeout
                logger.warning(f"Could not determine MKV file size, using base timeout: {conversion_timeout}s")
            
            logger.debug(f"Starting MKV->MP4 conversion with {conversion_timeout}s timeout for {self.user}")
            
            result = subprocess.run(
                convert_cmd,
                capture_output=True,
                text=True,
                timeout=conversion_timeout
            )
            
            if result.returncode == 0:
                # Verificar que el archivo MP4 se creó correctamente
                if os.path.exists(mp4_file) and os.path.getsize(mp4_file) > 0:
                    file_size_mb = os.path.getsize(mp4_file) / (1024 * 1024)
                    
                    logger.info(f"MKV->MP4 conversion successful: {mp4_file} ({file_size_mb:.1f}MB)")
                    
                    # Verificación condicional de integridad del MP4
                    if config.enable_ffprobe_verification:
                        verify_cmd = [
                            'ffprobe',
                            '-v', 'quiet',
                            '-show_entries', 'stream=codec_type,codec_name',
                            '-of', 'csv=p=0',
                            mp4_file
                        ]
                        
                        try:
                            verify_result = subprocess.run(verify_cmd, capture_output=True, text=True, timeout=10)
                            if verify_result.returncode == 0:
                                streams = verify_result.stdout.strip().split('\n') if verify_result.stdout.strip() else []
                                video_streams = [s for s in streams if 'video,' in s]
                                audio_streams = [s for s in streams if 'audio,' in s]
                                
                                if len(video_streams) == 0:
                                    logger.error(f"CRITICAL: No video streams detected in {mp4_file} - conversion may have failed!")
                                if len(audio_streams) == 0:
                                    logger.warning(f"Warning: No audio streams found in {mp4_file}")
                            else:
                                logger.error(f"MP4 verification failed for {mp4_file}")
                                logger.error(f"Verify stderr: {verify_result.stderr}")
                                return False
                        except Exception as verify_error:
                            logger.warning(f"Could not verify MP4 {mp4_file}: {verify_error}")
                    
                    # CRÍTICO: Limpiar archivo .mkv temporal después de conversión exitosa
                    try:
                        # Normalize path for Windows compatibility
                        normalized_path = os.path.normpath(mkv_file)
                        if os.path.exists(normalized_path):
                            os.remove(normalized_path)
                            logger.debug(f"Cleaned up temporary MKV file after conversion: {mkv_file}")
                    except Exception as cleanup_error:
                        logger.warning(f"Could not cleanup MKV file {mkv_file}: {cleanup_error}")
                    
                    return True
                else:
                    logger.error(f"MKV->MP4 conversion failed - no valid MP4 output: {mp4_file}")
                    self._cleanup_corrupted_mkv_file(mkv_file, "invalid MP4 output")
                    return False
            else:
                logger.error(f"MKV->MP4 conversion failed with return code {result.returncode}")
                logger.error(f"FFmpeg stderr: {result.stderr}")
                self._cleanup_corrupted_mkv_file(mkv_file, f"FFmpeg error (code {result.returncode})")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"MKV->MP4 conversion timeout for {self.user}")
            self._cleanup_corrupted_mkv_file(mkv_file, "conversion timeout")
            return False
        except Exception as e:
            logger.error(f"MKV->MP4 conversion error for {self.user}: {e}")
            self._cleanup_corrupted_mkv_file(mkv_file, f"conversion exception: {e}")
            return False

    def _cleanup_corrupted_mkv_file(self, mkv_file: str, reason: str):
        """
        Limpia archivos MKV corruptos que no se pueden convertir a MP4

        Args:
            mkv_file: Ruta al archivo MKV corrupto
            reason: Razón de la falla de conversión
        """
        try:
            # Normalize path for Windows compatibility
            normalized_path = os.path.normpath(mkv_file)

            if os.path.exists(normalized_path):
                file_size_mb = os.path.getsize(normalized_path) / (1024 * 1024)

                # Registrar información del archivo antes de eliminarlo
                logger.warning(f"🗑️ Deleting corrupted MKV file: {os.path.basename(mkv_file)} ({file_size_mb:.2f}MB) - Reason: {reason}")

                os.remove(normalized_path)
                logger.info(f"✅ Successfully deleted corrupted MKV file: {os.path.basename(mkv_file)}")

            else:
                logger.debug(f"🔍 MKV file already doesn't exist, no cleanup needed: {mkv_file}")

        except Exception as e:
            logger.error(f"❌ Failed to cleanup corrupted MKV file {mkv_file}: {e}")

    def _terminate_ffmpeg_cleanly(self, ffmpeg_process, reason):
        """
        Termina FFmpeg con timeout de gracia de 30s para evitar corrupción
        CRÍTICO: Evitar errores que puedan cerrar el programa principal
        
        Args:
            ffmpeg_process: Proceso FFmpeg a terminar
            reason: Razón de la terminación para logging
        """
        try:
            logger.info(f"Starting graceful FFmpeg termination for {self.user} - Reason: {reason}")
            
            # Detener sampling en tiempo real cuando se termina FFmpeg
            if hasattr(self, 'video_sampler'):
                self.video_sampler.stop_sampling()
            
            # FASE 1: Comando 'q' con timeout de gracia extendido
            try:
                logger.info(f"Sending 'q' command to FFmpeg for {self.user}")
                ffmpeg_process.stdin.write('q\n')
                ffmpeg_process.stdin.flush()
                
                # TIMEOUT DE GRACIA: 30 segundos para que FFmpeg escriba headers finales
                grace_timeout = config.ffmpeg_grace_timeout_seconds
                logger.info(f"Waiting {grace_timeout}s for FFmpeg to complete container headers for {self.user}")
                ffmpeg_process.wait(timeout=grace_timeout)
                logger.info(f"FFmpeg terminated cleanly via stdin for {self.user}")
                return
                
            except subprocess.TimeoutExpired:
                logger.warning(f"FFmpeg did not respond to 'q' command within {grace_timeout}s for {self.user}")
            except (BrokenPipeError, OSError) as e:
                logger.warning(f"Could not send 'q' to FFmpeg stdin for {self.user}: {e}")
            
            # FASE 2: Señales con cuidado especial para evitar cerrar el programa
            if ffmpeg_process.poll() is None:  # Aún está corriendo
                try:
                    if os.name == 'nt':  # Windows
                        logger.info(f"Using SIGTERM for Windows FFmpeg termination: {self.user}")
                        ffmpeg_process.terminate()
                    else:  # Linux/Mac
                        logger.info(f"Using SIGINT for Unix FFmpeg termination: {self.user}")
                        ffmpeg_process.send_signal(signal.SIGINT)
                    
                    # Timeout adicional para señales
                    ffmpeg_process.wait(timeout=10)
                    logger.info(f"FFmpeg terminated via signal for {self.user}")
                    return
                    
                except subprocess.TimeoutExpired:
                    logger.warning(f"FFmpeg did not respond to signals within 10s for {self.user}")
                except Exception as signal_error:
                    logger.error(f"Error sending signal to FFmpeg for {self.user}: {signal_error}")
            
            # FASE 3: Kill como último recurso (con protección)
            if ffmpeg_process.poll() is None:
                logger.warning(f"Force killing FFmpeg process for {self.user} (last resort)")
                try:
                    ffmpeg_process.kill()
                    ffmpeg_process.wait(timeout=5)
                    logger.warning(f"FFmpeg force killed for {self.user}")
                except Exception as kill_error:
                    logger.error(f"Could not kill FFmpeg process for {self.user}: {kill_error}")
            
        except Exception as e:
            # CRÍTICO: Capturar cualquier error para evitar cerrar el programa principal
            logger.error(f"Critical error in FFmpeg termination for {self.user}: {e}")
            try:
                # Intento final de cleanup
                if ffmpeg_process and ffmpeg_process.poll() is None:
                    ffmpeg_process.kill()
                    ffmpeg_process.wait(timeout=3)
            except Exception as final_error:
                logger.error(f"Final cleanup error for {self.user}: {final_error}")
                # NO re-raise la excepción para evitar cerrar el programa
        
        finally:
            # CRÍTICO: Cerrar pipes explícitamente para evitar file descriptor leaks
            if ffmpeg_process:
                try:
                    if ffmpeg_process.stdin and not ffmpeg_process.stdin.closed:
                        ffmpeg_process.stdin.close()
                except:
                    pass
                try:
                    if ffmpeg_process.stdout and not ffmpeg_process.stdout.closed:
                        ffmpeg_process.stdout.close()
                except:
                    pass
                try:
                    if ffmpeg_process.stderr and not ffmpeg_process.stderr.closed:
                        ffmpeg_process.stderr.close()
                except:
                    pass
    
    def _validate_mp4_file(self, mp4_path):
        """
        Valida que el archivo MP4 esté completo y no corrupto
        Usa validación tolerante con reintentos para archivos recién grabados
        
        Args:
            mp4_path: Ruta al archivo MP4
            
        Returns:
            bool: True si el archivo es válido, False si está corrupto
        """
        # Validación básica de archivo
        if not os.path.exists(mp4_path):
            logger.error(f"MP4 file does not exist: {mp4_path}")
            return False
        
        file_size = os.path.getsize(mp4_path)
        if file_size < 1024:  # <1KB definitivamente corrupto
            logger.error(f"MP4 file too small: {mp4_path} ({file_size} bytes)")
            return False
        
        # Intentar validación con ffprobe (con reintentos para archivos recién escritos)
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                import ffmpeg
                
                # Pequeña pausa para archivos recién escritos
                if attempt > 0:
                    time.sleep(0.5)
                
                # Usar ffprobe para verificar la integridad del archivo
                probe = ffmpeg.probe(mp4_path)
                
                # Verificar que tenga streams de video
                video_streams = [stream for stream in probe['streams'] if stream['codec_type'] == 'video']
                if not video_streams:
                    if attempt == max_attempts - 1:
                        logger.warning(f"No video streams found in {mp4_path} after {max_attempts} attempts")
                        # TOLERANTE: Si tiene tamaño razonable, asumir que es válido
                        if file_size > 100 * 1024:  # >100KB
                            logger.info(f"MP4 file validation: Assuming valid due to size ({file_size/1024/1024:.1f}MB)")
                            return True
                        return False
                    continue
                
                # Verificar duración (más tolerante)
                duration = float(probe['format'].get('duration', 0))
                if duration <= 0:
                    if attempt == max_attempts - 1:
                        logger.warning(f"Invalid duration in {mp4_path}: {duration}")
                        # TOLERANTE: Si tiene tamaño razonable, asumir que es válido
                        if file_size > 100 * 1024:  # >100KB
                            logger.info(f"MP4 file validation: Assuming valid due to size ({file_size/1024/1024:.1f}MB)")
                            return True
                        return False
                    continue
                
                logger.info(f"MP4 file validation passed: {mp4_path} ({duration:.1f}s, {file_size/1024/1024:.1f}MB)")
                return True
                
            except Exception as e:
                if attempt == max_attempts - 1:
                    logger.warning(f"MP4 file validation failed for {mp4_path} after {max_attempts} attempts: {e}")
                    # TOLERANTE: Si el archivo tiene un tamaño razonable, asumir que está bien
                    # Esto evita bloquear uploads de videos válidos por problemas de ffprobe
                    if file_size > 100 * 1024:  # >100KB
                        logger.info(f"MP4 file validation: Bypassing ffprobe error due to reasonable size ({file_size/1024/1024:.1f}MB)")
                        return True
                    else:
                        logger.error(f"MP4 file validation: File too small and ffprobe failed, marking as corrupted")
                        return False
                else:
                    logger.debug(f"MP4 validation attempt {attempt + 1} failed: {e}, retrying...")
        
        return False

    def _validate_stream_continuation(self):
        """
        Valida si el stream continúa activo después de una terminación inesperada
        Espera el tiempo completo configurado verificando periódicamente con retry logic
        Siempre valida durante todo el tiempo configurado sin terminación temprana
        
        Returns:
            bool: True si el stream sigue activo, False si terminó definitivamente
        """
        validation_minutes = config.post_recording_validation_minutes
        check_interval = 40  # Verificar cada 40 segundos
        total_checks = (validation_minutes * 60) // check_interval
        
        logger.info(f"🔍 Starting stream continuation validation for {self.user} for {validation_minutes} minutes ({total_checks} checks every {check_interval}s)...")
        
        consecutive_failed_checks = 0
        
        for check in range(total_checks):
            try:
                if self.stop_flag:
                    logger.info(f"Stop flag detected during validation for {self.user}")
                    return False
                
                # Verificar si el usuario sigue en vivo CON RETRY LOGIC
                is_alive = self._check_room_alive_with_retry()
                
                if is_alive:
                    logger.info(f"✅ Stream confirmed active for {self.user} (check {check + 1}/{total_checks}) - validation successful!")
                    return True
                else:
                    consecutive_failed_checks += 1
                    logger.warning(f"❌ Stream check {check + 1}/{total_checks}: user {self.user} not detected as live (consecutive failures: {consecutive_failed_checks})")
                
                # Pausa antes del siguiente check (excepto en el último)
                if check < total_checks - 1:
                    time.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"Error during stream validation for {self.user}: {e}")
                consecutive_failed_checks += 1
                if check < total_checks - 1:
                    time.sleep(check_interval)
        
        logger.info(f"🏁 Stream validation completed for {self.user} after {validation_minutes} minutes - stream appears to have ended (consecutive failures: {consecutive_failed_checks})")
        return False
    
    def _check_room_alive_with_retry(self, max_retries=1, retry_delay=5):
        """
        Verifica si la room está viva con retry logic para manejar fallas temporales de API
        Obtiene room_id fresco para detectar live streams reiniciados correctamente
        
        Args:
            max_retries: Número máximo de reintentos
            retry_delay: Delay entre reintentos en segundos
            
        Returns:
            bool: True si la room está viva, False si definitivamente no
        """
        logger.debug(f"🔍 Checking if room is alive for {self.user} (current room_id: {self.room_id}) with {max_retries} retries...")
        
        for attempt in range(max_retries):
            try:
                # CAMBIO CRÍTICO: Obtener room_id fresco del username actual
                # Esto detecta correctamente cuando el usuario reinicia live con nuevo room_id
                current_room_id = self.tiktok.get_room_id_from_user(self.user)
                
                if current_room_id:
                    # Validar con room_id actual
                    is_alive = self.tiktok.is_room_alive(current_room_id)
                    
                    if is_alive:
                        if current_room_id != self.room_id:
                            logger.info(f"🔄 Room ID updated for {self.user}: {self.room_id} → {current_room_id}")
                            self.room_id = current_room_id
                            self.cached_stream_url = None
                            self.stream_url_cache_time = None
                        
                        logger.info(f"✅ Stream alive confirmed for {self.user} with room_id {current_room_id} (attempt {attempt + 1}/{max_retries})")
                        return True
                
                # Si no se obtiene room_id o no está vivo, reintentar
                if attempt < max_retries - 1:
                    logger.debug(f"❌ Room alive check failed for {self.user}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                
            except Exception as e:
                logger.warning(f"⚠️ Room alive check error for {self.user} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        
        logger.info(f"❌ Room alive check failed for {self.user} after {max_retries} attempts (total time: {max_retries * retry_delay}s)")
        return False
    
    
    def _start_continuous_recording_part(self):
        """
        Inicia una nueva parte de grabación continua
        """
        try:
            # CRÍTICO: Verificar stop flag antes de continuar
            if self.stop_flag:
                logger.info(f"Stop flag detected, triggering final processing for {self.user}")
                self._trigger_final_processing()
                return
                
            # CRÍTICO: Resetear flag de memory restart para nuevo segmento
            self.memory_restart_triggered = False
            
            self.current_part_number += 1
            self.is_continuous_session = True
            
            logger.info(f"Starting continuous recording part {self.current_part_number} for {self.user}")
            
            # Pequeña pausa antes de reiniciar
            time.sleep(config.auto_restart_delay_seconds)
            
            # CRÍTICO: Verificar stop flag después del delay también
            if self.stop_flag:
                logger.info(f"Stop flag detected during delay, triggering final processing for {self.user}")
                self._trigger_final_processing()
                return
            
            # Obtener URL del stream 
            try:
                # Si fue error de URL expirada, validar que el usuario siga en vivo antes de obtener URL fresca
                if self.url_expiration_detected:
                    logger.info(f"🔄 URL expiration detected, validating live status for {self.user}")
                    
                    # Validar que el usuario aún esté en vivo antes de intentar obtener URL fresca
                    is_still_live = self._check_room_alive_with_retry()
                    if not is_still_live:
                        logger.info(f"User {self.user} is no longer live after URL expiration - ending recording")
                        self.url_expiration_detected = False
                        self._trigger_final_processing()
                        return
                    
                    # Solo si sigue en vivo: obtener URL fresca
                    logger.info(f"User {self.user} confirmed live, fetching fresh URL")
                    live_url = self._get_fresh_stream_url()
                    self.url_expiration_detected = False  # Reset flag
                else:
                    # Usar cache normal
                    live_url = self._get_stream_url_cached()
                
                if not live_url:
                    logger.info(f"Could not get stream URL for {self.user}, stream ended - processing existing parts")
                    self._trigger_final_processing()
                    return
            except Exception as url_error:
                logger.info(f"Stream ended for {self.user}, stopping continuous recording: {url_error}")
                self._trigger_final_processing()
                return
            
            # Generar nombre del archivo para la nueva parte
            # CRÍTICO: Usar timestamp actual para el archivo, pero mantener carpeta de sesión
            current_date = time.strftime("%Y.%m.%d_%H-%M-%S", time.localtime())
            
            # Usar estructura de carpetas organizadas (reutiliza carpeta existente)
            # NOTA: session_folder_path ya está establecido, no necesita current_date
            organized_output_path = self._create_organized_output_path(None)
            
            # Generar nombres según el patrón requerido: _cont1_part2, _cont2_part2, etc.
            if self.fragment_number > 1:
                continuous_suffix = f"_cont{self.current_part_number}_part{self.fragment_number}"
            else:
                continuous_suffix = f"_cont{self.current_part_number}" if self.current_part_number > 1 else ""
            output = f"{organized_output_path}TK_{self.user}_{current_date}{continuous_suffix}_flv.mp4"
            
            logger.info(f"Continuous recording part {self.current_part_number}: {output}")
            
            # Reiniciar grabación FFmpeg
            self._start_ffmpeg_direct_recording(live_url, output)
            
        except Exception as e:
            logger.error(f"Error starting continuous recording part for {self.user}: {e}")

    def _trigger_final_processing(self):
        """
        Activa el procesamiento final de todas las partes de grabación existentes.
        Utiliza la cola de post-procesamiento para grabaciones con múltiples segmentos.
        """
        logger.info(f"Triggering final processing for {self.user} with {len(self.recording_parts)} parts")
        
        # CRÍTICO: Procesar partes existentes SIEMPRE, incluso si la grabación actual falló
        if len(self.recording_parts) > 0:
            # PRIMERO: Convertir todos los archivos .mkv a .mp4
            converted_parts = []
            for i, mkv_part in enumerate(self.recording_parts):
                if mkv_part.endswith('.mkv'):
                    mp4_part = mkv_part.replace('.mkv', '.mp4')
                    logger.info(f"Converting part {i+1}/{len(self.recording_parts)}: {mkv_part} -> {mp4_part}")
                    if self._convert_mkv_to_mp4(mkv_part, mp4_part):
                        converted_parts.append(mp4_part)
                        # Limpiar archivo .mkv temporal
                        try:
                            # Normalize path for Windows compatibility
                            normalized_path = os.path.normpath(mkv_part)
                            os.remove(normalized_path)
                            logger.debug(f"Cleaned up temporary MKV file: {mkv_part}")
                        except Exception as cleanup_error:
                            logger.warning(f"Could not cleanup MKV file {mkv_part}: {cleanup_error}")
                    else:
                        logger.error(f"Failed to convert {mkv_part} to MP4, skipping this part")
                else:
                    # Ya es MP4, añadir directamente
                    converted_parts.append(mkv_part)
            
            # Actualizar recording_parts con los archivos MP4 convertidos
            self.recording_parts = converted_parts
            logger.info(f"Converted all parts to MP4. Final parts count: {len(self.recording_parts)}")
            
            if len(self.recording_parts) > 0:
                # Usar la cola de post-procesamiento asíncrona
                asyncio.run(self._submit_to_postprocessing_queue(is_fragmentation=False))
            else:
                logger.warning(f"No valid recording parts available for {self.user}")
        else:
            logger.warning(f"No recording parts to process for {self.user}")
        
        # Limpiar estado de sesión
        self._cleanup_session_state()
        
        # Ejecutar callback de limpieza si está definido
        if self.cleanup_callback:
            logger.info(f"Executing cleanup callback for {self.user}")
            self.cleanup_callback()

    async def _submit_to_postprocessing_queue(self, is_fragmentation=False):
        """
        Envía la grabación a la cola de post-procesamiento asíncrona
        
        Args:
            is_fragmentation: True si es un fragmento por tamaño, False si es grabación completa
        """
        try:
            from services.postprocessing_queue import postprocessing_queue
            
            # Determinar si debería mostrar "Part X" en Telegram
            # Mostrar "Part X" cuando:
            # 1. Hay fragmentación REAL por tamaño de archivo (is_fragmentation=True), O
            # 2. El fragmento actual es > 1 (indica que ya había partes previas fragmentadas)
            should_show_part_number = is_fragmentation or self.fragment_number > 1
    
            # Preparar información de salida
            output_info = {
                'telegram_chat_id': self.telegram_chat_id,
                'fragment_number': self.fragment_number,
                'use_telegram': self.use_telegram,
                'telegram_instance': self.telegram,
                'is_fragmented': should_show_part_number
            }
            
            # Enviar a la cola (se procesará inmediatamente si < min_segments_for_queue)
            job_id = await postprocessing_queue.add_recording_for_postprocessing(
                self.user,
                self.recording_parts,
                output_info
            )
            
            if job_id:
                logger.info(f"📥 Recording submitted to post-processing queue: {job_id}")
            else:
                logger.info(f"⚡ Recording processed immediately (< {config.min_segments_for_queue} segments)")
                
        except Exception as e:
            logger.error(f"Error submitting to post-processing queue for {self.user}: {e}")
            # Fallback al procesamiento tradicional
            await self._fallback_to_traditional_processing()

    async def _fallback_to_traditional_processing(self):
        """
        Procesamiento tradicional como fallback cuando falla la cola
        """
        logger.info(f"Using traditional processing as fallback for {self.user}")
        
        try:
            if len(self.recording_parts) > 1 and config.enable_smart_concatenation:
                logger.info(f"Multiple recording parts detected for {self.user}, concatenating...")
                final_video = self._concatenate_recording_parts()
                if final_video and os.path.exists(final_video):
                    logger.info(f"📁 CONCATENATED VIDEO READY: {final_video}")
                    self._process_final_output(final_video, is_continuous=True)
                else:
                    # Si falla la concatenación, usar la parte más grande
                    logger.warning(f"Concatenation failed for {self.user}, using largest part as fallback")
                    largest_part = self._get_largest_recording_part()
                    if largest_part:
                        logger.info(f"📁 LARGEST PART READY: {largest_part}")
                        self._process_final_output(largest_part)
                    else:
                        logger.error(f"No valid recording parts available for {self.user}")
            elif len(self.recording_parts) == 1:
                # Una sola parte
                single_part = self.recording_parts[0]
                logger.info(f"Single recording part for {self.user}: {single_part}")
                self._process_final_output(single_part)
        except Exception as e:
            logger.error(f"Fallback processing failed for {self.user}: {e}")
    
    def _process_all_parts_immediately(self, is_fragmentation=False):
        """
        Procesa todos los segmentos inmediatamente para fragmentación por tamaño
        Convierte MKV a MP4 y envía a post-procesamiento
        """
        try:
            logger.info(f"Converting {len(self.recording_parts)} parts from MKV to MP4 for immediate processing")
            
            converted_parts = []
            # Convertir cada parte MKV a MP4
            for i, mkv_part in enumerate(self.recording_parts):
                if mkv_part.endswith('.mkv'):
                    mp4_part = mkv_part.replace('.mkv', '.mp4')
                    logger.info(f"Converting part {i+1}/{len(self.recording_parts)}: {mkv_part} -> {mp4_part}")
                    if self._convert_mkv_to_mp4(mkv_part, mp4_part):
                        converted_parts.append(mp4_part)
                        # Limpiar archivo .mkv temporal
                        try:
                            # Normalize path for Windows compatibility
                            normalized_path = os.path.normpath(mkv_part)
                            os.remove(normalized_path)
                            logger.debug(f"Cleaned up temporary MKV file: {mkv_part}")
                        except Exception as cleanup_error:
                            logger.warning(f"Could not cleanup MKV file {mkv_part}: {cleanup_error}")
                    else:
                        logger.error(f"Failed to convert {mkv_part} to MP4, skipping this part")
                else:
                    # Ya es MP4, añadir directamente
                    converted_parts.append(mkv_part)
            
            # Actualizar recording_parts con los archivos MP4 convertidos
            self.recording_parts = converted_parts
            logger.info(f"Converted all parts to MP4. Final parts count: {len(self.recording_parts)}")
            
            if len(self.recording_parts) > 0:
                # Enviar a la cola de post-procesamiento (se procesará inmediatamente por ser pocos segmentos)
                asyncio.run(self._submit_to_postprocessing_queue(is_fragmentation=is_fragmentation))
            else:
                logger.warning(f"No valid recording parts available for {self.user}")
                
        except Exception as e:
            logger.error(f"Error in immediate processing for {self.user}: {e}")
    
    def _concatenate_recording_parts(self):
        """
        Concatena todas las partes de grabación de la misma sesión en un solo video
        
        Returns:
            str: Ruta al video concatenado final, None si falla
        """
        try:
            if len(self.recording_parts) < 2:
                return self.recording_parts[0] if self.recording_parts else None
            
            import tempfile
            import ffmpeg
            
            # Validar que todas las partes existan y tengan suficientes frames
            valid_parts = []
            pause_videos = []  # Videos de pausa para limpiar
            total_filtered_size = 0
            
            for part in self.recording_parts:
                if os.path.exists(part):
                    part_size = os.path.getsize(part) / (1024*1024)
                    
                    # Validar frames del video
                    frame_validation = self._validate_video_frames(part)
                    
                    # Si hay timeout, asumir que es válido para evitar archivos huérfanos
                    # Los errores de validación se mantienen como inválidos
                    if frame_validation['valid'] or 'timeout' in frame_validation['reason'].lower():
                        valid_parts.append(part)
                        if 'timeout' in frame_validation['reason'].lower():
                            logger.info(f"✅ Valid recording part (timeout assumed valid): {part} ({part_size:.1f}MB)")
                        else:
                            logger.info(f"✅ Valid recording part: {part} ({part_size:.1f}MB, {frame_validation['frames']} frames)")
                    else:
                        pause_videos.append(part)
                        total_filtered_size += part_size
                        logger.warning(f"🚨 Filtered pause video: {part} ({part_size:.1f}MB) - {frame_validation['reason']}")
                else:
                    logger.warning(f"❌ Missing recording part: {part}")
            
            # Limpiar videos de pausa inmediatamente si está habilitado
            if pause_videos and config.auto_cleanup_pause_videos:
                logger.info(f"🧹 Cleaning up {len(pause_videos)} pause videos ({total_filtered_size:.1f}MB total)")
                for pause_video in pause_videos:
                    try:
                        if os.path.exists(pause_video):
                            os.remove(pause_video)
                            logger.info(f"🗑️  Deleted pause video: {pause_video}")
                    except Exception as e:
                        logger.error(f"Could not delete pause video {pause_video}: {e}")
            
            logger.info(f"📊 Frame validation summary: {len(valid_parts)} valid parts, {len(pause_videos)} pause videos filtered")
            
            if len(valid_parts) < 2:
                logger.warning(f"Not enough valid parts to concatenate for {self.user}")
                return valid_parts[0] if valid_parts else None
            
            # Generar nombre del archivo final ÚNICO (no sobrescribir ninguna parte)
            first_part = valid_parts[0]
            # Extraer el nombre base sin extensiones de continuación
            base_name = first_part.replace('_flv.mp4', '').replace('.mp4', '')
            # Remover sufijos de continuación si existen
            if '_cont' in base_name:
                base_name = base_name.split('_cont')[0]
            
            # Crear nombre único para el archivo concatenado
            final_output = f"{base_name}_COMPLETE.mp4"
            
            # Crear archivo de lista temporal para FFmpeg
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
                for part in valid_parts:
                    escaped_path = part.replace('\\', '/').replace("'", "\\'")
                    f.write(f"file '{escaped_path}'\n")
                list_file = f.name
            
            try:
                logger.info(f"Concatenating {len(valid_parts)} recording parts for {self.user}...")
                
                # Concatenar usando FFmpeg
                ffmpeg.input(list_file, format='concat', safe=0).output(
                    final_output,
                    c='copy',
                    avoid_negative_ts='make_zero',
                    movflags='+faststart'
                ).run(quiet=True, overwrite_output=True)
                
                # Verificar que el archivo concatenado se creó correctamente ANTES de limpiar
                if os.path.exists(final_output) and os.path.getsize(final_output) > 1024:  # >1KB
                    final_size = os.path.getsize(final_output) / (1024*1024)
                    logger.info(f"Recording parts concatenated successfully: {final_output} ({final_size:.1f}MB)")
                    
                    # CRÍTICO: Solo limpiar partes después de verificar que la concatenación funcionó
                    logger.info(f"Cleaning up {len(valid_parts)} individual parts after successful concatenation")
                    for part in valid_parts:
                        try:
                            if os.path.exists(part):
                                part_size = os.path.getsize(part) / (1024*1024)
                                os.remove(part)
                                logger.debug(f"Cleaned up recording part: {part} ({part_size:.1f}MB)")
                        except Exception as e:
                            logger.warning(f"Could not remove recording part {part}: {e}")
                    
                    return final_output
                else:
                    # Concatenación falló - NO limpiar las partes originales
                    if os.path.exists(final_output):
                        final_size = os.path.getsize(final_output)
                        logger.error(f"Concatenation failed for {self.user} - output file too small ({final_size} bytes)")
                    else:
                        logger.error(f"Concatenation failed for {self.user} - no output file created")
                    
                    logger.warning(f"Keeping original parts due to concatenation failure")
                    return None
                
            finally:
                # Limpiar archivo de lista
                try:
                    os.unlink(list_file)
                except Exception as e:
                    logger.warning(f"Could not remove temp list file: {e}")
            
        except Exception as e:
            logger.error(f"Error concatenating recording parts for {self.user}: {e}")
            return None

    def _validate_video_frames(self, video_path):
        """
        Valida que un video tenga suficientes frames (no sea video de pausa)
        
        Args:
            video_path: Ruta al archivo de video
            
        Returns:
            dict: {'valid': bool, 'frames': int, 'duration': float, 'reason': str}
        """
        try:
            if not config.enable_frame_validation:
                return {'valid': True, 'frames': -1, 'duration': 0, 'reason': 'Frame validation disabled'}
            
            if not os.path.exists(video_path):
                return {'valid': False, 'frames': 0, 'duration': 0, 'reason': 'File does not exist'}
            
            import subprocess
            
            # Comando para contar frames
            cmd_frames = [
                'ffprobe', '-v', 'error', 
                '-select_streams', 'v:0', 
                '-count_frames', 
                '-show_entries', 'stream=nb_frames', 
                '-of', 'csv=p=0', 
                video_path
            ]
            
            # Comando para obtener duración
            cmd_duration = [
                'ffprobe', '-v', 'error',
                '-show_entries', 'stream=duration',
                '-of', 'csv=p=0',
                video_path
            ]
            
            # Ejecutar comandos con timeout
            result_frames = subprocess.run(cmd_frames, capture_output=True, text=True, timeout=10)
            result_duration = subprocess.run(cmd_duration, capture_output=True, text=True, timeout=10)
            
            frames = 0
            duration = 0.0
            
            # Procesar resultado de frames
            if result_frames.returncode == 0 and result_frames.stdout.strip():
                try:
                    frames = int(result_frames.stdout.strip())
                except ValueError:
                    frames = 0
            
            # Procesar resultado de duración
            if result_duration.returncode == 0 and result_duration.stdout.strip():
                try:
                    duration = float(result_duration.stdout.strip())
                except ValueError:
                    duration = 0.0
            
            # Validaciones
            file_size = os.path.getsize(video_path)
            min_frames = config.min_frames_threshold
            
            # Video de pausa: pocos frames, duración larga, archivo pequeño
            if frames <= min_frames and duration > 30:  # >30s con ≤5 frames
                reason = f"Pause video: {frames} frames in {duration:.1f}s"
                logger.info(f"🚨 Pause video detected: {video_path} - {reason}")
                return {'valid': False, 'frames': frames, 'duration': duration, 'reason': reason}
            
            # Video muy pequeño
            elif file_size < 1024:  # <1KB
                reason = f"File too small: {file_size} bytes"
                logger.debug(f"Small file detected: {video_path} - {reason}")
                return {'valid': False, 'frames': frames, 'duration': duration, 'reason': reason}
            
            # Video válido
            else:
                reason = f"Valid video: {frames} frames, {duration:.1f}s"
                logger.debug(f"Valid video: {video_path} - {reason}")
                return {'valid': True, 'frames': frames, 'duration': duration, 'reason': reason}
            
        except subprocess.TimeoutExpired:
            logger.warning(f"Timeout validating frames for {video_path}")
            return {'valid': True, 'frames': -1, 'duration': 0, 'reason': 'Validation timeout'}
        except Exception as e:
            logger.error(f"Error validating frames for {video_path}: {e}")
            return {'valid': True, 'frames': -1, 'duration': 0, 'reason': f'Validation error: {e}'}

    def _get_largest_recording_part(self):
        """
        Obtiene la parte de grabación más grande como fallback
        
        Returns:
            str: Ruta al archivo más grande, None si no hay partes válidas
        """
        try:
            if not self.recording_parts:
                return None
            
            largest_part = None
            largest_size = 0
            
            for part in self.recording_parts:
                if os.path.exists(part):
                    part_size = os.path.getsize(part)
                    if part_size > largest_size:
                        largest_size = part_size
                        largest_part = part
            
            if largest_part:
                size_mb = largest_size / (1024*1024)
                logger.info(f"Largest recording part: {largest_part} ({size_mb:.1f}MB)")
                return largest_part
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding largest recording part: {e}")
            return None
        
    def _generate_video_thumbnail(self, video_path: str) -> Optional[str]:
        """
        Genera un thumbnail random del video grabado
        
        Args:
            video_path: Ruta al archivo de video MP4
            
        Returns:
            str: Ruta al archivo de thumbnail generado, None si falla
        """
        try:
            import random
            import ffmpeg
            
            # Obtener duración del video
            probe = ffmpeg.probe(video_path)
            video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
            
            if not video_stream:
                logger.error(f"No video stream found in {video_path}")
                return None
                
            duration = float(video_stream.get('duration', 0))
            
            if duration < 5:  # Video muy corto
                timestamp = duration / 2  # Mitad del video
            else:
                # Timestamp random entre el 10% y 90% del video (evitar inicio/final)
                start_time = duration * 0.1
                end_time = duration * 0.9
                timestamp = random.uniform(start_time, end_time)
            
            # Generar nombre del archivo de thumbnail
            video_dir = os.path.dirname(video_path)
            video_name = os.path.splitext(os.path.basename(video_path))[0]
            thumbnail_path = os.path.join(video_dir, f"{video_name}_thumb.jpg")
            
            logger.debug(f"Generating random thumbnail at {timestamp:.1f}s for {self.user}")
            
            # Extraer screenshot usando FFmpeg con alta calidad
            ffmpeg.input(video_path, ss=timestamp).output(
                thumbnail_path,
                vframes=1,
                format='image2',
                vcodec='mjpeg',
                q=2,  # Alta calidad
                s='640x1138'  # Resolución vertical optimizada para thumbnail
            ).run(quiet=True, overwrite_output=True)
            
            if os.path.exists(thumbnail_path) and os.path.getsize(thumbnail_path) > 0:
                logger.debug(f"Video thumbnail generated successfully: {thumbnail_path}")
                return thumbnail_path
            else:
                logger.warning(f"Failed to generate video thumbnail for {self.user}")
                return None
                
        except Exception as e:
            logger.error(f"Error generating video thumbnail for {self.user}: {e}")
            return None

    def _generate_video_collage(self, video_path: str) -> Optional[str]:
        """
        Genera un collage de screenshots del video grabado
        
        Args:
            video_path: Ruta al archivo de video MP4
            
        Returns:
            str: Ruta al archivo de collage generado, None si falla
        """
        try:
            collage_generator = VideoCollageGenerator()
            
            # Generar nombre del archivo de collage
            video_dir = os.path.dirname(video_path)
            video_name = os.path.splitext(os.path.basename(video_path))[0]
            collage_path = os.path.join(video_dir, f"{video_name}_collage.jpg")
            
            logger.info(f"Generating video collage for {self.user}...")
            
            if collage_generator.generate_collage(video_path, collage_path):
                logger.info(f"Video collage generated successfully: {collage_path}")
                return collage_path
            else:
                logger.warning(f"Failed to generate video collage for {self.user}")
                return None
                
        except Exception as e:
            logger.error(f"Error generating video collage for {self.user}: {e}")
            return None
    def _create_organized_output_path(self, current_date):
        """
        Crea estructura de carpetas organizadas: OUTPUT_DIRECTORY/usuario-fecha-hora/
        Reutiliza la misma carpeta para todos los segmentos de la sesión.
        
        Args:
            current_date: Fecha actual en formato "YYYY.MM.DD_HH-MM-SS" (puede ser None para reutilizar)
            
        Returns:
            str: Ruta completa con carpetas creadas y separador final
        """
        try:
            # Si la organización de carpetas está deshabilitada, usar comportamiento original
            if not config.enable_organized_folders:
                fallback_path = self.output if self.output else config.output_directory
                if not fallback_path.endswith(('/', '\\')):
                    fallback_path += os.sep
                return fallback_path
            
            # Si ya tenemos una carpeta de sesión creada, reutilizarla SIEMPRE
            if self.session_folder_path:
                logger.debug(f"Reusing session folder: {self.session_folder_path}")
                return self.session_folder_path
            
            # SOLO crear nueva carpeta si no existe (primera vez)
            # Formatear fecha y hora para nombre de carpeta usando la fecha de inicio de sesión
            session_date = time.strftime("%Y.%m.%d_%H-%M-%S", time.localtime(self.session_start_time))
            formatted_datetime = session_date.replace('.', '-')
            
            # Construir nombre de carpeta: usuario-fecha-hora (SIEMPRE basado en session_start_time)
            folder_name = f"{self.user}-{formatted_datetime}"
            
            # Construir ruta completa: OUTPUT_DIRECTORY/usuario-fecha-hora/
            base_output = self.output if self.output else config.output_directory
            if not base_output.endswith(('/', '\\')):
                base_output += os.sep
            
            recording_folder = os.path.join(base_output, folder_name)
            
            # Crear carpeta si no existe
            os.makedirs(recording_folder, exist_ok=True)
            
            # Asegurar separador final
            if not recording_folder.endswith(('/', '\\')):
                recording_folder += os.sep
            
            # Guardar la carpeta de sesión para reutilizar
            self.session_folder_path = recording_folder
            
            logger.info(f"📁 Created session folder: {recording_folder}")
            logger.debug(f"🔧 Session folder will be reused for all parts of this recording session")
            return recording_folder
            
        except Exception as e:
            logger.error(f"Error creating organized output path: {e}")
            # Fallback a comportamiento original
            fallback_path = self.output if self.output else config.output_directory
            if not fallback_path.endswith(('/', '\\')):
                fallback_path += os.sep
            return fallback_path

    def check_country_blacklisted(self):
        is_blacklisted = self.tiktok.is_country_blacklisted()
        if not is_blacklisted:
            return False

        if self.room_id is None:
            raise TikTokException(TikTokError.COUNTRY_BLACKLISTED)

        if self.mode == Mode.AUTOMATIC:
            raise TikTokException(TikTokError.COUNTRY_BLACKLISTED_AUTO_MODE)

    def _monitor_ffmpeg_stderr(self, ffmpeg_process):
        """
        Hilo guardian que monitorea stderr de FFmpeg en tiempo real para detectar corrupción
        Analiza patrones específicos de errores que indican problemas de stream
        
        Args:
            ffmpeg_process: Proceso FFmpeg a monitorear
        """
        try:
            logger.info(f"🔍 Starting FFmpeg stderr monitor for {self.user}")
            
            # Patrones de corrupción críticos para detectar
            corruption_patterns = [
                # PTS/DTS errors (muy comunes en lives de TikTok)
                #r"Non-monotonous DTS in output stream",
                #r"Non-monotonic DTS.*changing to.*This may result in incorrect timestamps",  # NUEVO: DTS no monotónico
                #r"PTS < DTS in output stream",
                #r"Invalid DTS:",
                #r"Invalid PTS:",
                #r"DTS out of order",
                #r"PTS out of order",
                
                # Timestamp corruption
               # r"Application provided invalid, non monotonically increasing dts",
               # r"Timestamps are unset in a packet",
               # r"Encoder did not produce proper pts",
                
                # Stream corruption
                #r"corrupt decoded frame",
                #r"concealing errors",
                #r"error while decoding",
                #r"Invalid data found when processing input",
                #r"corrupt input packet",
                #r"missing picture in access unit",
                
                # H.264 specific errors
                #r"error unpacking bitsream",
                #r"Invalid NAL unit size",
                #r"decode_slice_header error",
                #r"corrupted macroblock",
                #r"mmco: unref short failure",
                #r"\d+ bytes left at end of AVCC header",  # NUEVO: Header AVCC corrupto (filtrado especialmente)
                
                # Audio corruption
                r"Frame size larger than expected",
                r"Audio packet of size .* is invalid",
                r"invalid frame size",
                
                # Connection/stream errors that indicate unstable stream
                r"Connection timed out",
                r"Server returned 4[0-9][0-9]",
                r"Server returned 5[0-9][0-9]",
                r"HTTP error 4[0-9][0-9]",
                r"HTTP error 5[0-9][0-9]",
                
                # TLS/Encryption errors that require immediate restart
                r"Decryption has failed",  # Critical TLS error that corrupts entire stream
                r"Will reconnect at .* error=Input/output error",  # Pre-TLS failure indicator
                r"Packet mismatch \d+ \d+ \d+",  # FLV packet corruption (often precedes TLS failure)
                r"Concatenated FLV detected.*might fail",  # FLV stream corruption
                
                # Matroska timestamp corruption (CRITICAL - causes huge temporal gaps)
                #r"Starting new cluster due to timestamp"  # Matroska timestamp corruption - immediate restart needed
                
                # Buffer overruns/underruns
                r"Real-time buffer .* too full or near too full",
                r"Queue input is backward in time",
                
                # Problemas de stream y conexión más específicos
                r"Last message repeated",  # FFmpeg detecta repeticiones
                r"Past duration .* too large",  # Problemas de duración
                r"Decoder did not produce an output frame",  # Frames perdidos
                r"stream.*error.*\b(failed|corrupt|invalid|timeout|denied)",  # Errores de stream más específicos
                r"stream.*fail.*\b(connection|network|read|write)",   # Fallos de stream específicos
                r"dropping.*packet.*\b(corrupt|invalid|malformed)",  # Solo paquetes problemáticos
                r"skipping.*frame.*\b(corrupt|invalid|error)",   # Solo frames con problemas
                r"unexpected.*EOF.*\b(stream|connection|network)",   # EOF relacionado con problemas de red
                r"connection.*\b(failed|timeout|refused|reset)",     # Fallos de conexión específicos
                r"reconnecting.*\b(due to|after|failed)",    # Reconexiones por problemas
                r"packet too large",  # Paquetes malformados
                r"Invalid frame size"  # Tamaño de frame inválido
            ]
            
            import re
            compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in corruption_patterns]
            
            consecutive_errors = 0
            total_errors = 0
            error_window_start = time.time()
            
            # Variables para detectar degradación de performance
            last_speed = 1.0
            performance_degradation_count = 0
            
            # Variables para detectar timestamp repetition (UNIVERSAL - Windows y Linux)
            last_timestamp = None
            repeated_timestamp_count = 0
            
            # Variables para filtrar errores de inicialización
            stream_start_time = time.time()
            initialization_period = 2  # CRÍTICO: Solo primeros 2 segundos son período de inicialización
            avcc_header_errors = 0  # Contador específico para errores de AVCC header
            
            # Leer stderr línea por línea
            for line in ffmpeg_process.stderr:
                if not line:
                    continue
                
                # Limpiar la línea
                clean_line = line.strip()
                if not clean_line:  # Saltar líneas completamente vacías
                    continue
                    
                # Escribir TODA línea en tiempo real al archivo de log
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                log_entry = f"[{timestamp}] {clean_line}"
                
                # Guardar en memoria para resumen final (con rotación para prevenir acumulación)
                self.ffmpeg_stderr_log.append(log_entry)
                
                # NUEVO: Rotación de log en memoria para prevenir acumulación excesiva
                if len(self.ffmpeg_stderr_log) > self.max_stderr_log_entries:
                    # Mantener solo las últimas entradas y rotar
                    self.ffmpeg_stderr_log = self.ffmpeg_stderr_log[-int(self.max_stderr_log_entries/2):]
                    self.stderr_log_rotation_count += 1
                    logger.debug(f"FFmpeg stderr log rotated (#{self.stderr_log_rotation_count}) to prevent memory accumulation")
                
                # ESCRIBIR TODAS LAS LÍNEAS EN TIEMPO REAL al archivo
                if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
                    try:
                        self.ffmpeg_log_file.write(log_entry + "\n")
                        self.ffmpeg_log_file.flush()  # Forzar escritura inmediata
                    except Exception as write_error:
                        logger.warning(f"Could not write to FFmpeg log file: {write_error}")
                
                # PRIMERO: Verificar si es error de URL expirada (antes de corruption patterns)
                url_error_patterns = [
                    r"HTTP error 404",
                    r"HTTP error 403", 
                    r"HTTP error 400",
                    r"Server returned 404",
                    r"Server returned 403",
                    r"Server returned 400",
                    r"Error opening input.*404",
                    r"Error opening input.*403",
                    r"Connection refused",
                    r"Connection reset",
                    r"Network is unreachable"
                ]
                
                is_url_error = False
                for pattern in url_error_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        logger.warning(f"🔄 URL EXPIRATION DETECTED for {self.user}: {line.strip()}")
                        self.url_expiration_detected = True
                        is_url_error = True
                        # Escribir marcador de URL expiration en tiempo real
                        url_marker = f"[{timestamp}] *** URL EXPIRATION DETECTED *** {line.strip()}"
                        self.ffmpeg_stderr_log.append(url_marker)
                        if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
                            try:
                                self.ffmpeg_log_file.write(url_marker + "\n")
                                self.ffmpeg_log_file.flush()
                            except Exception as write_error:
                                logger.warning(f"Could not write URL expiration marker: {write_error}")
                        break
                
                # Si es error de URL, NO procesarlo como corruption pattern
                if is_url_error:
                    continue
                
                # SEGUNDO: Verificar patrones de corrupción (solo si NO es error de URL)
                corruption_found = False
                current_time = time.time()
                
                for pattern in compiled_patterns:
                    if pattern.search(line):
                        # FILTROS ESPECIALES: Errores comunes de inicialización del stream
                        is_initialization_error = False
                        
                        # Error de AVCC header - filtrar solo durante inicialización
                        if "bytes left at end of AVCC header" in line:
                            # Durante período de inicialización (primeros 2s), ser más tolerante
                            if current_time - stream_start_time < initialization_period:
                                avcc_header_errors += 1
                                if avcc_header_errors <= 1:  # REDUCIDO: Permitir solo 1 error AVCC al inicio
                                    is_initialization_error = True
                                    logger.debug(f"AVCC header error during initialization (#{avcc_header_errors}): {line.strip()}")
                                    # Escribir al log pero NO contar como corrupción
                                    if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
                                        try:
                                            init_marker = f"[{timestamp}] *** INITIALIZATION AVCC ERROR (#{avcc_header_errors}) *** {line.strip()}"
                                            self.ffmpeg_log_file.write(init_marker + "\n")
                                            self.ffmpeg_log_file.flush()
                                        except Exception as write_error:
                                            logger.warning(f"Could not write initialization marker: {write_error}")
                                # Si ya pasamos el límite de 3 errores en inicialización, tratar como corrupción
                                else:
                                    logger.warning(f"AVCC errors exceeded initialization limit (#{avcc_header_errors}) - treating as corruption")
                            # DESPUÉS de 30s: AVCC header errors son corrupción real inmediatamente
                            # No marcar como initialization_error, permitir que se procese como corrupción normal
                        
                        # DTS no monotónico también puede ser común al inicio
                        elif "Non-monotonic DTS" in line and "changing to" in line:
                            # Durante inicialización, ser tolerante con DTS no monotónico ocasional
                            if current_time - stream_start_time < initialization_period:
                                is_initialization_error = True
                                logger.debug(f"Non-monotonic DTS during initialization: {line.strip()}")
                                if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
                                    try:
                                        init_marker = f"[{timestamp}] *** INITIALIZATION DTS ERROR *** {line.strip()}"
                                        self.ffmpeg_log_file.write(init_marker + "\n")
                                        self.ffmpeg_log_file.flush()
                                    except Exception as write_error:
                                        logger.warning(f"Could not write initialization marker: {write_error}")
                        
                        # Si es error de inicialización, no contar como corrupción
                        if is_initialization_error:
                            continue
                        
                        # Error válido de corrupción
                        consecutive_errors += 1
                        total_errors += 1
                        corruption_found = True
                        
                        logger.warning(f"🚨 CORRUPTION PATTERN DETECTED for {self.user}: {line.strip()}")
                        
                        # Escribir marcador de corrupción en tiempo real
                        corruption_marker = f"[{timestamp}] *** CORRUPTION DETECTED *** {line.strip()}"
                        self.ffmpeg_stderr_log.append(corruption_marker)
                        
                        if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
                            try:
                                self.ffmpeg_log_file.write(corruption_marker + "\n")
                                self.ffmpeg_log_file.flush()
                            except Exception as write_error:
                                logger.warning(f"Could not write corruption marker to log: {write_error}")
                        
                        # CRÍTICO: Errores graves que requieren reinicio inmediato después de inicialización
                        critical_errors_immediate_restart = [
                            #"bytes left at end of AVCC header",  # AVCC header corruption después de init
                            "corrupt decoded frame",
                            "Invalid data found when processing input",
                            "Decryption has failed",  # TLS decryption failure - immediate restart required
                            "Will reconnect at",  # Pre-TLS failure - restart before corruption
                            #"Packet mismatch",  # FLV corruption - restart immediately
                            #"Concatenated FLV detected",  # Stream corruption - restart immediately
                            #"Starting new cluster due to timestamp"  # Matroska timestamp corruption - CRITICAL
                        ]
                        
                        # Si es un error crítico después del período de inicialización, reiniciar inmediatamente
                        if (current_time - stream_start_time >= initialization_period and 
                            any(critical_error in line for critical_error in critical_errors_immediate_restart)):
                            logger.error(f"🚨 CRITICAL ERROR DETECTED for {self.user}, triggering immediate restart: {line.strip()}")
                            
                            # Marcar específicamente errores AVCC para postprocesado
                            if "bytes left at end of AVCC header" in line:
                                self.avcc_error_detected = True
                                logger.warning(f"🩹 AVCC error detected - will trigger postprocessing for {self.user}")
                            
                            # Marcar específicamente errores de timestamp matroska para eliminación inmediata
                            if "Starting new cluster due to timestamp" in line:
                                logger.error(f"🕳️ MATROSKA TIMESTAMP CORRUPTION detected for {self.user} - segment will be discarded after FFmpeg terminates")
                                # No eliminar inmediatamente, sino marcar para eliminación posterior
                                self.matroska_corruption_detected = True
                            
                            self.corruption_detected = True
                            break
                        
                        break
                
                # DETECCIÓN DE ANOMALÍAS DE SPEED (independiente de frame/fps para compatibilidad Linux)
                if "speedX3=" in clean_line:
                    try:
                        # Extraer información de speed y bitrate (disponibles en Linux)
                        speed_match = re.search(r'speed=\s*(\d+(?:\.\d+)?)x', clean_line)
                        speed_na_match = re.search(r'speed=N/A', clean_line)
                        bitrate_match = re.search(r'bitrate=\s*(\d+(?:\.\d+)?)kbits/s', clean_line)
                        size_match = re.search(r'size=\s*(\d+)kB', clean_line)
                        
                        # DETECCIÓN CRÍTICA: speed=N/A (precursor de fallas TLS)
                        if speed_na_match:
                            logger.error(f"🚨 CRITICAL: Speed=N/A detected for {self.user} - TLS failure imminent")
                            
                            corruption_marker = f"[{timestamp}] *** CRITICAL SPEED N/A *** TLS failure precursor detected"
                            self.ffmpeg_stderr_log.append(corruption_marker)
                            
                            if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
                                try:
                                    self.ffmpeg_log_file.write(corruption_marker + "\n")
                                    self.ffmpeg_log_file.flush()
                                except Exception as write_error:
                                    logger.warning(f"Could not write speed N/A marker: {write_error}")
                            
                            # Marcar para reinicio inmediato
                            self.corruption_detected = True
                            break
                        
                        # DETECCIÓN DE ANOMALÍAS DE SPEED NUMERICAS
                        if speed_match:
                            current_speed = float(speed_match.group(1))
                            
                            # DETECCIÓN CRÍTICA: speed extremo (>1000000x indica corrupción TLS)
                            if current_speed > 1000000:  # Speed imposiblemente alto
                                logger.error(f"🚨 CRITICAL: Extreme speed detected for {self.user}: {current_speed}x - TLS corruption")
                                
                                corruption_marker = f"[{timestamp}] *** EXTREME SPEED *** {current_speed}x detected - TLS corruption"
                                self.ffmpeg_stderr_log.append(corruption_marker)
                                
                                if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
                                    try:
                                        self.ffmpeg_log_file.write(corruption_marker + "\n")
                                        self.ffmpeg_log_file.flush()
                                    except Exception as write_error:
                                        logger.warning(f"Could not write extreme speed marker: {write_error}")
                                
                                # Marcar para reinicio inmediato
                                self.corruption_detected = True
                                break
                            
                            # DETECCIÓN: Speed anómalamente alto (>50x indica problemas de conexión)
                            elif current_speed > 50.0:
                                logger.warning(f"🚨 CRITICAL SPEED ANOMALY for {self.user}: {current_speed}x (>50x threshold)")
                                
                                corruption_marker = f"[{timestamp}] *** SPEED ANOMALY DETECTED *** Speed: {current_speed}x - potential TLS failure precursor"
                                self.ffmpeg_stderr_log.append(corruption_marker)
                                
                                if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
                                    try:
                                        self.ffmpeg_log_file.write(corruption_marker + "\n")
                                        self.ffmpeg_log_file.flush()
                                    except Exception as write_error:
                                        logger.warning(f"Could not write speed anomaly marker: {write_error}")
                                
                                consecutive_errors += 1
                                total_errors += 1
                                corruption_found = True
                            
                            # DETECCIÓN: Degradación severa de performance (speed drop severo)
                            elif current_speed < 0.5 and last_speed >= 0.5:
                                performance_degradation_count += 1
                                logger.warning(f"🚨 PERFORMANCE DEGRADATION for {self.user}: Speed dropped to {current_speed}x")
                                
                                corruption_marker = f"[{timestamp}] *** PERFORMANCE DEGRADATION *** Speed: {current_speed}x"
                                self.ffmpeg_stderr_log.append(corruption_marker)
                                
                                if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
                                    try:
                                        self.ffmpeg_log_file.write(corruption_marker + "\n")
                                        self.ffmpeg_log_file.flush()
                                    except Exception as write_error:
                                        logger.warning(f"Could not write performance degradation marker: {write_error}")
                                
                                # CRÍTICO: También contar como error de corrupción
                                consecutive_errors += 1
                                total_errors += 1
                                corruption_found = True
                            
                            # Actualizar last_speed para próxima comparación
                            last_speed = current_speed
                        
                        # DETECCIÓN CRÍTICA: bitrate=0 y size=0 (precursores de falla TLS - REINICIO INMEDIATO)
                        if bitrate_match and size_match:
                            current_bitrate = float(bitrate_match.group(1))
                            current_size = int(size_match.group(1))
                            
                            if current_bitrate == 0.0 and current_size == 0:
                                logger.error(f"🚨 CRITICAL: Zero bitrate and size detected for {self.user} - TLS failure imminent, triggering immediate restart")
                                
                                corruption_marker = f"[{timestamp}] *** ZERO BITRATE/SIZE *** bitrate: {current_bitrate}, size: {current_size}kB - TLS failure precursor"
                                self.ffmpeg_stderr_log.append(corruption_marker)
                                
                                if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
                                    try:
                                        self.ffmpeg_log_file.write(corruption_marker + "\n")
                                        self.ffmpeg_log_file.flush()
                                    except Exception as write_error:
                                        logger.warning(f"Could not write zero bitrate marker: {write_error}")
                                
                                # Marcar para reinicio inmediato (evitar llegar a "Decryption has failed")
                                self.corruption_detected = True
                                break
                        
                    except (ValueError, AttributeError) as e:
                        logger.debug(f"Error parsing speed line: {e}")
                
                # DETECCIÓN UNIVERSAL DE REPETICIÓN DE TIMESTAMP (Windows y Linux)
                # Extraer timestamp del progress line (disponible en ambos sistemas)
                time_match = re.search(r'time=(\d{2}:\d{2}:\d{2}\.\d{2})', clean_line)
                if time_match and config.enable_timestamp_frame_detection:
                    current_timestamp = time_match.group(1)
                    
                    # Detectar repetición del mismo timestamp (indica frames estancados/corrupción)
                    if last_timestamp and current_timestamp == last_timestamp:
                        repeated_timestamp_count += 1
                        logger.warning(f"🚨 REPEATED TIMESTAMP DETECTED for {self.user}: time={current_timestamp} (#{repeated_timestamp_count})")
                        
                        if repeated_timestamp_count >= config.frame_repetition_threshold:
                            logger.error(f"🚨 CRITICAL: Timestamp repetition threshold reached for {self.user} - triggering immediate restart")
                            
                            corruption_marker = f"[{timestamp}] *** CRITICAL TIMESTAMP REPETITION *** time={current_timestamp} repeated {repeated_timestamp_count} times"
                            self.ffmpeg_stderr_log.append(corruption_marker)
                            
                            if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
                                try:
                                    self.ffmpeg_log_file.write(corruption_marker + "\n")
                                    self.ffmpeg_log_file.flush()
                                except Exception as write_error:
                                    logger.warning(f"Could not write timestamp repetition marker: {write_error}")
                            
                            # Marcar como corrupción crítica para reinicio inmediato
                            self.corruption_detected = True
                            self.corruption_restart_pending = True
                            logger.error(f"🚨 CORRUPTION RESTART triggered by timestamp repetition for {self.user}")
                            break
                    else:
                        repeated_timestamp_count = 0  # Reset si timestamp cambia
                    
                    last_timestamp = current_timestamp
                
                # GAP DETECTION: Detectar gaps significativos entre timestamps
                if config.enable_gap_detection:
                    timestamp_seconds, is_valid = self._parse_timestamp_safely(clean_line)
                    
                    if is_valid:
                        current_time_real = time.time()
                        
                        # Si han pasado >5 minutos sin timestamp válido, reset para evitar false positives
                        if (self.last_timestamp_update and 
                            current_time_real - self.last_timestamp_update > 300):
                            logger.debug(f"Resetting timestamp tracking after long N/A period for {self.user}")
                            self.last_valid_timestamp = timestamp_seconds
                        
                        # Detectar gap significativo
                        elif self.last_valid_timestamp is not None:
                            gap_seconds = timestamp_seconds - self.last_valid_timestamp
                            
                            # Detectar gap >threshold segundos
                            if gap_seconds > config.gap_detection_threshold_seconds:
                                logger.error(f"🚨 TIMESTAMP GAP DETECTED for {self.user}: {gap_seconds:.1f}s gap ({self.last_valid_timestamp:.1f}s → {timestamp_seconds:.1f}s)")
                                
                                gap_marker = f"[{timestamp}] *** TIMESTAMP GAP DETECTED *** {gap_seconds:.1f}s gap from {self.last_valid_timestamp:.1f}s to {timestamp_seconds:.1f}s"
                                self.ffmpeg_stderr_log.append(gap_marker)
                                
                                if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
                                    try:
                                        self.ffmpeg_log_file.write(gap_marker + "\n")
                                        self.ffmpeg_log_file.flush()
                                    except Exception as write_error:
                                        logger.warning(f"Could not write gap marker to log: {write_error}")
                                
                                # Guardar información para trimming
                                self.gap_detected = True
                                self.gap_start_timestamp = self.last_valid_timestamp
                                
                                # Marcar para reinicio inmediato
                                self.corruption_detected = True
                                logger.error(f"🚨 GAP RESTART triggered for {self.user} - will trim at {self.last_valid_timestamp:.1f}s")
                                break
                        
                        # Actualizar último timestamp válido y tiempo de update
                        self.last_valid_timestamp = timestamp_seconds
                        self.last_timestamp_update = current_time_real
                        
                    elif not is_valid and clean_line and 'time=' in clean_line:
                        # Log casos problemáticos para debugging
                        logger.debug(f"Invalid timestamp format skipped for {self.user}: {clean_line.strip()}")
                
                # Si no hay corrupción encontrada en esta línea, resetear contador consecutivo
                if not corruption_found:
                    consecutive_errors = 0
                
                # Verificar si hemos alcanzado el umbral de corrupción
                current_time = time.time()
                
                # Estrategia 1: Errores consecutivos
                if consecutive_errors >= self.corruption_threshold:
                    logger.error(f"🚨 CORRUPTION THRESHOLD REACHED for {self.user}: {consecutive_errors} consecutive errors")
                    self.corruption_detected = True
                    break
                
                # Estrategia 2: Ventana de tiempo (muchos errores en poco tiempo)
                if current_time - error_window_start > 20:  # REDUCIDO: Ventana de 20 segundos
                    if total_errors > 5:  # REDUCIDO: Más de 5 errores en 20 segundos
                        logger.error(f"🚨 HIGH ERROR RATE for {self.user}: {total_errors} errors in 20 seconds")
                        self.corruption_detected = True
                        break
                    # Reset window
                    error_window_start = current_time
                    total_errors = 0
                
                # Log importante para debugging (solo errores críticos en consola)
                # NOTA: Todo se está escribiendo al archivo, esto es solo para consola
                if any(keyword in clean_line.lower() for keyword in ['error', 'corrupt', 'invalid', 'failed', 'warning']):
                    logger.debug(f"FFmpeg stderr [{self.user}]: {clean_line}")
                
                # Log de progreso ocasional para confirmar que está funcionando
                if "frame=" in clean_line and "fps=" in clean_line:
                    # Solo cada 100 frames para no saturar logs
                    try:
                        if "frame=" in clean_line:
                            frame_part = clean_line.split("frame=")[1].split()[0].strip()
                            if frame_part.isdigit() and int(frame_part) % 300 == 0:  # Cada 300 frames (~10s)
                                logger.debug(f"FFmpeg progress [{self.user}]: {clean_line}")
                    except:
                        pass
        
        except Exception as e:
            logger.error(f"Error in FFmpeg stderr monitor for {self.user}: {e}")
        
        finally:
            # Cerrar archivo de log si está abierto
            if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
                try:
                    self.ffmpeg_log_file.write(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] FFmpeg monitoring finished\n")
                    self.ffmpeg_log_file.write(f"Corruption detected during session: {self.corruption_detected}\n")
                    self.ffmpeg_log_file.close()
                except Exception as e:
                    logger.warning(f"Error closing FFmpeg log file: {e}")
            
            logger.info(f"🔍 FFmpeg stderr monitor finished for {self.user}")

    def _save_ffmpeg_stderr_log(self):
        """
        Genera resumen final del log de FFmpeg (el archivo ya se escribió en tiempo real)
        """
        try:
            if not self.ffmpeg_stderr_log_path or not os.path.exists(self.ffmpeg_stderr_log_path):
                return
            
            log_size = os.path.getsize(self.ffmpeg_stderr_log_path) / 1024  # KB
            logger.info(f"📄 FFmpeg stderr log completed: {self.ffmpeg_stderr_log_path} ({log_size:.1f}KB)")
            
            # Log resumen de lo que se encontró
            corruption_entries = [entry for entry in self.ffmpeg_stderr_log if "CORRUPTION DETECTED" in entry]
            if corruption_entries:
                logger.warning(f"🚨 CORRUPTION SUMMARY for {self.user}: {len(corruption_entries)} corruption patterns detected")
                logger.warning("📋 Please review the stderr log file for detailed analysis")
            else:
                logger.info(f"✅ No corruption patterns detected for {self.user}")
            
        except Exception as e:
            logger.error(f"Error generating FFmpeg stderr log summary for {self.user}: {e}")



    def _is_cached_url_valid(self) -> bool:
        """
        Verifica si la URL en cache está válida y no ha expirado
        
        Returns:
            bool: True si la URL cached es válida, False si no existe o expiró
        """
        if not self.cached_stream_url or not self.stream_url_cache_time:
            return False
            
        current_time = time.time()
        cache_age = current_time - self.stream_url_cache_time
        
        # Verificar si no ha expirado
        is_valid = cache_age < self.stream_url_cache_duration
        
        if is_valid:
            logger.debug(f"✅ Cached URL is valid for {self.user} (age: {cache_age:.1f}s, expires in: {self.stream_url_cache_duration - cache_age:.1f}s)")
        else:
            logger.debug(f"❌ Cached URL expired for {self.user} (age: {cache_age:.1f}s, duration: {self.stream_url_cache_duration}s)")
            
        return is_valid

    def _force_fresh_stream_url(self):
        """
        Fuerza obtención de URL completamente fresca para reinicio.
        Incluye delay para permitir a TikTok generar nueva URL válida.
        """
        logger.info(f"🔄 Forcing fresh stream URL for restart: {self.user}")
        
        # Invalidar cache completamente
        self.cached_stream_url = None
        self.stream_url_cache_time = None
        
        # Delay para permitir a TikTok generar nueva URL
        url_refresh_delay = config.force_url_refresh_delay_seconds
        logger.info(f"⏳ Waiting {url_refresh_delay}s for URL refresh...")
        time.sleep(url_refresh_delay)
        
        # Obtener URL fresca (se cachea automáticamente)
        try:
            fresh_url = self._get_stream_url_cached()
            if fresh_url:
                logger.info(f"✅ Fresh stream URL obtained for {self.user}")
                return fresh_url
            else:
                logger.error(f"❌ Could not obtain fresh URL for {self.user}")
                return None
        except Exception as e:
            logger.error(f"❌ Error getting fresh URL for {self.user}: {e}")
            return None

    def _attempt_corruption_restart(self) -> bool:
        """
        Intenta reiniciar grabación después de corrupción.
        Returns: True si reinicio exitoso, False si falló
        """
        try:
            # Marcar para reinicio por corrupción
            self.corruption_restart_pending = True
            
            # Esperar estabilización
            self._wait_for_stream_stabilization()
            
            # Verificar stop flag después de estabilización
            if self.stop_flag:
                logger.info(f"Stop flag detected during stabilization for {self.user}")
                return False
            
            # Intentar iniciar nueva parte
            logger.info(f"🔄 Starting continuous recording for corruption restart - Part {self.current_part_number}")
            self._start_continuous_recording_part()
            
            # Si llegamos aquí sin excepción, el restart fue exitoso
            return True
            
        except Exception as e:
            logger.error(f"Error during restart attempt for {self.user}: {e}")
            return False

    def _wait_for_stream_stabilization(self):
        """
        Espera a que el stream se estabilize después de detectar corrupción
        """
        logger.info(f"⏳ Waiting {self.stabilization_wait_time}s for stream stabilization for {self.user}")
        time.sleep(self.stabilization_wait_time)

    def _monitor_ffmpeg_memory(self, process_pid):
        """
        Monitorea el uso de memoria del proceso FFmpeg y trigger restart si supera umbral
        
        Args:
            process_pid: PID del proceso FFmpeg a monitorear
        """
        if not config.enable_ffmpeg_memory_guardian:
            logger.debug(f"FFmpeg memory guardian disabled for {self.user}")
            return
            
        if not psutil:
            logger.warning(f"psutil not available, skipping memory monitoring for {self.user}")
            return
            
        try:
            process = psutil.Process(process_pid)
            total_memory = psutil.virtual_memory().total
            max_memory_percent = config.ffmpeg_memory_threshold_percent
            check_interval = config.memory_check_interval_seconds
            
            logger.info(f"🔍 Started memory guardian for {self.user} (PID: {process_pid}, threshold: {max_memory_percent}%)")
            
            while process.is_running() and not self.stop_flag and not self.memory_restart_triggered:
                try:
                    memory_info = process.memory_info()
                    memory_percent = (memory_info.rss / total_memory) * 100
                    
                    # Log memory usage periodically (every 5 checks = ~50 seconds)
                    if hasattr(self, '_memory_check_count'):
                        self._memory_check_count += 1
                    else:
                        self._memory_check_count = 1
                        
                    if self._memory_check_count % 5 == 0:
                        memory_mb = memory_info.rss / (1024 * 1024)
                        logger.debug(f"📊 Memory usage for {self.user}: {memory_percent:.2f}% ({memory_mb:.1f}MB)")
                    
                    # Check if memory usage exceeds threshold
                    if memory_percent > max_memory_percent:
                        memory_mb = memory_info.rss / (1024 * 1024)
                        logger.warning(f"🚨 FFmpeg memory usage {memory_percent:.2f}% ({memory_mb:.1f}MB) exceeds threshold {max_memory_percent}% for {self.user}")
                        
                        # Trigger memory restart
                        self.memory_restart_triggered = True
                        self._trigger_memory_restart()
                        break
                        
                    time.sleep(check_interval)
                    
                except psutil.NoSuchProcess:
                    logger.debug(f"FFmpeg process {process_pid} ended, stopping memory monitor for {self.user}")
                    break
                except psutil.AccessDenied:
                    logger.warning(f"Access denied to monitor FFmpeg process {process_pid} for {self.user}")
                    break
                    
        except psutil.NoSuchProcess:
            logger.debug(f"FFmpeg process {process_pid} not found, memory monitor stopped for {self.user}")
        except Exception as e:
            logger.error(f"Error in memory guardian for {self.user}: {e}")

    def _trigger_memory_restart(self):
        """
        Trigger restart debido a exceso de memoria - usar path de corrupción para continuación
        """
        try:
            logger.warning(f"🚨 MEMORY RESTART: Flagging for restart due to memory usage for {self.user}")
            # CAMBIO: Usar corruption path en lugar de fragmentation path
            self.corruption_detected = True
            
            # Terminar FFmpeg limpiamente
            if hasattr(self, 'ffmpeg_process') and self.ffmpeg_process and self.ffmpeg_process.poll() is None:
                logger.info(f"🛑 Terminating FFmpeg process due to memory usage for {self.user}")
                self._terminate_ffmpeg_cleanly(self.ffmpeg_process, "memory usage exceeded")
                
        except Exception as e:
            logger.error(f"Error during memory restart trigger for {self.user}: {e}")

    def _get_stream_url_cached(self):
        """
        Obtiene URL del stream con cache para evitar peticiones excesivas a TikTok.
        Reutiliza URL mientras siga siendo válida para reducir rate limiting.
        
        Returns:
            str: URL del stream válida
        """
        current_time = time.time()
        
        # Verificar si tenemos URL cached válida
        if (self.cached_stream_url and 
            self.stream_url_cache_time and 
            (current_time - self.stream_url_cache_time) < self.stream_url_cache_duration):
            
            cache_age = current_time - self.stream_url_cache_time
            logger.info(f"🔄 Reusing cached stream URL for {self.user} (age: {cache_age:.1f}s)")
            return self.cached_stream_url
        
        # Cache expirado o no existe, obtener nueva URL
        try:
            logger.info(f"🌐 Fetching fresh stream URL for {self.user}")
            fresh_url = self.tiktok.get_live_url(self.room_id, user=self.user)
            
            if fresh_url:
                # Actualizar cache
                self.cached_stream_url = fresh_url
                self.stream_url_cache_time = current_time
                logger.info(f"✅ Stream URL cached for {self.user} (valid for {self.stream_url_cache_duration/60:.1f} minutes)")
                return fresh_url
            else:
                logger.error(f"❌ Failed to get fresh stream URL for {self.user}")
                return None
                
        except TikTokLiveRateLimitException:
            logger.error(f"TikTokLive rate limit reached for {self.user} - propagating to bot handler")
            raise
        except Exception as e:
            logger.error(f"Error getting fresh stream URL for {self.user}: {e}")
            # Si hay error pero tenemos cache (aunque expirado), usarlo como fallback
            if self.cached_stream_url:
                logger.warning(f"⚠️ Using expired cached URL as fallback for {self.user}")
                return self.cached_stream_url
            return None

    def _get_fresh_stream_url(self):
        """
        Fuerza la obtención de una nueva URL del stream (bypasea cache)
        Útil cuando la URL actual ha expirado o es inválida
        
        Returns:
            str: Nueva URL del stream o None si falló
        """
        try:
            logger.info(f"🔄 Forcing fresh stream URL fetch for {self.user} (bypassing cache)")
            
            # Limpiar cache actual
            self.cached_stream_url = None
            self.stream_url_cache_time = None
            
            # Obtener nueva URL directamente
            fresh_url = self.tiktok.get_live_url(self.room_id, user=self.user)
            
            if fresh_url:
                # Actualizar cache con la nueva URL
                self.cached_stream_url = fresh_url
                self.stream_url_cache_time = time.time()
                logger.info(f"✅ Fresh stream URL obtained for {self.user}")
                return fresh_url
            else:
                logger.error(f"❌ Failed to get fresh stream URL for {self.user}")
                return None
                
        except Exception as e:
            logger.error(f"Error forcing fresh stream URL for {self.user}: {e}")
            return None

    def _initialize_session_state_file(self):
        """
        Inicializa archivo de estado persistente para la sesión de grabación
        Esto previene pérdida de partes en grabaciones largas
        """
        try:
            # Crear directorio de estado si no existe
            base_output = self.output if self.output else config.output_directory
            if not base_output.endswith(('/', '\\')):
                base_output += os.sep
                
            state_dir = os.path.join(base_output, "recording_states")
            os.makedirs(state_dir, exist_ok=True)
            
            # Crear archivo de estado para esta sesión
            self.state_file_path = os.path.join(state_dir, f"session_{self.session_id}.json")
            
            # Inicializar con metadatos de la sesión
            session_state = {
                "session_id": self.session_id,
                "user": self.user,
                "room_id": self.room_id,
                "telegram_chat_id": self.telegram_chat_id,  # CRÍTICO: Para recuperación de uploads
                "start_time": self.session_start_time,
                "current_part_number": self.current_part_number,
                "recording_parts": [],
                "status": "active",
                "last_update": time.time()
            }
            
            self._save_session_state(session_state)
            logger.info(f"📄 Session state file created: {self.state_file_path}")
            
        except Exception as e:
            logger.error(f"Failed to initialize session state file: {e}")
            self.state_file_path = None

    def _save_session_state(self, state_data=None):
        """
        Guarda el estado actual de la sesión al archivo persistente
        """
        if not self.state_file_path:
            return
            
        try:
            if state_data is None:
                # Generar estado actual
                state_data = {
                    "session_id": self.session_id,
                    "user": self.user,
                    "room_id": self.room_id,
                    "telegram_chat_id": self.telegram_chat_id,  # CRÍTICO: Para recuperación de uploads
                    "start_time": self.session_start_time,
                    "current_part_number": self.current_part_number,
                    "recording_parts": self.recording_parts.copy(),
                    "status": "active",
                    "last_update": time.time(),
                    "total_parts": len(self.recording_parts)
                }
            
            import json
            with open(self.state_file_path, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, indent=2, ensure_ascii=False)
                
            logger.debug(f"Session state saved: {len(self.recording_parts)} parts")
            
        except Exception as e:
            logger.error(f"Failed to save session state: {e}")

    def _load_session_state(self):
        """
        Carga el estado de la sesión desde el archivo persistente
        Útil para recuperación después de reinicios o fallos
        """
        if not self.state_file_path or not os.path.exists(self.state_file_path):
            return None
            
        try:
            import json
            with open(self.state_file_path, 'r', encoding='utf-8') as f:
                state_data = json.load(f)
            
            # Validar que las partes existen físicamente
            valid_parts = []
            for part_path in state_data.get("recording_parts", []):
                if os.path.exists(part_path):
                    valid_parts.append(part_path)
                else:
                    logger.warning(f"Recording part not found: {part_path}")
            
            # Actualizar con partes válidas
            self.recording_parts = valid_parts
            self.current_part_number = state_data.get("current_part_number", 1)
            
            logger.info(f"📄 Session state loaded: {len(valid_parts)} valid parts recovered")
            return state_data
            
        except Exception as e:
            logger.error(f"Failed to load session state: {e}")
            return None

    def _cleanup_session_state(self):
        """
        Limpia el estado de la sesión de grabación
        """
        logger.info(f"Cleaning up session state for {self.user}")
        
        # Limpiar archivo de estado persistente
        self._cleanup_session_state_file()
        
        # Resetear variables de estado
        self.recording_parts = []
        self.is_continuous_session = False
        self.corruption_restart_pending = False
        self.corruption_detected = False
        self.avcc_error_detected = False
        self.matroska_corruption_detected = False
        self.corruption_count = 0
        
        # Limpiar carpeta de sesión si está usando carpetas organizadas
        self._cleanup_session_folder()
        
        # Cerrar archivo de log de FFmpeg si está abierto
        if hasattr(self, 'ffmpeg_log_file') and self.ffmpeg_log_file:
            try:
                self.ffmpeg_log_file.close()
                self.ffmpeg_log_file = None
            except Exception as e:
                logger.warning(f"Error closing FFmpeg log file: {e}")
        
        # CRÍTICO: Cerrar cliente HTTP de TikTok para evitar conexiones CLOSE_WAIT
        if hasattr(self, 'tiktok') and self.tiktok:
            try:
                self.tiktok.close()
                logger.debug(f"TikTok HTTP client closed for {self.user}")
            except Exception as e:
                logger.warning(f"Error closing TikTok HTTP client for {self.user}: {e}")

    def _cleanup_session_folder(self):
        """
        Limpia la carpeta de sesión después del upload exitoso (solo si está vacía)
        """
        if not self.session_folder_path or not config.enable_organized_folders:
            return
            
        try:
            if os.path.exists(self.session_folder_path):
                # Verificar si la carpeta está vacía
                folder_contents = os.listdir(self.session_folder_path)
                
                if not folder_contents:
                    # La carpeta está vacía, eliminarla
                    os.rmdir(self.session_folder_path)
                    logger.info(f"🗑️  Cleaned up empty session folder: {self.session_folder_path}")
                else:
                    # La carpeta tiene archivos, listarlos en debug
                    logger.debug(f"Session folder not empty, keeping: {self.session_folder_path} (contents: {folder_contents})")
                    
        except Exception as e:
            logger.warning(f"Could not cleanup session folder {self.session_folder_path}: {e}")
        finally:
            # Resetear la variable independientemente del resultado
            self.session_folder_path = None

    def _cleanup_session_state_file(self):
        """
        Marca el archivo de estado como completado pero NO lo elimina
        Solo se eliminará cuando el UploadManager confirme que el upload fue exitoso
        """
        if self.state_file_path and os.path.exists(self.state_file_path):
            try:
                # Marcar como completado pero preservar para upload recovery
                final_state = {
                    "session_id": self.session_id,
                    "user": self.user,
                    "status": "completed",
                    "completion_time": time.time(),
                    "total_parts_processed": len(self.recording_parts),
                    "telegram_chat_id": self.telegram_chat_id,  # Preservar chat_id para recovery
                    "start_time": self.session_start_time
                }
                self._save_session_state(final_state)
                logger.info(f"📄 Session state marked as completed (preserved for upload recovery): {self.session_id}")
                
            except Exception as e:
                logger.warning(f"Could not update session state file: {e}")

    def recover_recording_parts_from_state(self):
        """
        Recupera las partes de grabación desde el archivo de estado persistente
        Útil cuando se pierde el estado en memoria durante grabaciones largas
        
        Returns:
            bool: True si se recuperaron partes exitosamente
        """
        try:
            logger.info(f"🔄 Attempting to recover recording parts from state file for {self.user}")
            
            # Cargar estado desde archivo
            state_data = self._load_session_state()
            if not state_data:
                logger.warning(f"No state file found for recovery: {self.user}")
                return False
            
            # Verificar que realmente se recuperaron partes
            if len(self.recording_parts) == 0:
                logger.warning(f"No valid parts recovered from state file for {self.user}")
                return False
            
            logger.info(f"✅ Successfully recovered {len(self.recording_parts)} recording parts for {self.user}")
            
            # Actualizar estado después de recuperación
            self._save_session_state()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to recover recording parts from state: {e}")
            return False

    @staticmethod
    def get_orphaned_session_files(base_output_dir):
        """
        Encuentra archivos de sesión huérfanos (de grabaciones interrumpidas)
        Útil para limpieza manual o recuperación de emergencia
        
        Args:
            base_output_dir: Directorio base donde buscar archivos de estado
            
        Returns:
            List[dict]: Lista de sesiones huérfanas con metadatos
        """
        try:
            state_dir = os.path.join(base_output_dir, "recording_states")
            if not os.path.exists(state_dir):
                return []
            
            orphaned_sessions = []
            current_time = time.time()
            
            for filename in os.listdir(state_dir):
                if filename.startswith("session_") and filename.endswith(".json"):
                    state_file_path = os.path.join(state_dir, filename)
                    
                    try:
                        import json
                        with open(state_file_path, 'r', encoding='utf-8') as f:
                            state_data = json.load(f)
                        
                        # Considerar huérfana si está "active" y tiene más de 2 horas sin update
                        last_update = state_data.get("last_update", 0)
                        status = state_data.get("status", "unknown")
                        
                        if status == "active" and (current_time - last_update) > 7200:  # 2 horas
                            orphaned_sessions.append({
                                "file_path": state_file_path,
                                "session_id": state_data.get("session_id"),
                                "user": state_data.get("user"),
                                "start_time": state_data.get("start_time"),
                                "last_update": last_update,
                                "parts_count": len(state_data.get("recording_parts", [])),
                                "hours_stale": (current_time - last_update) / 3600
                            })
                            
                    except Exception as e:
                        logger.warning(f"Could not read state file {filename}: {e}")
            
            return orphaned_sessions
            
        except Exception as e:
            logger.error(f"Error finding orphaned session files: {e}")
            return []
    
    def _handle_sampler_corruption_detected(self):
        """
        Callback ejecutado cuando el RealtimeVideoSampler detecta corrupción silenciosa.
        Corta el segmento actual e inicia un nuevo segmento de grabación.
        """
        try:
            logger.warning(f"🚨 SILENT CORRUPTION DETECTED by sampler for {self.user} - forcing segment restart")
            
            # Marcar que el próximo restart es por corrupción detectada por sampler
            self.corruption_restart_pending = True
            
            # Activar flag de corrupción para que el bucle principal maneje el restart
            self.corruption_detected = True
            
            logger.info(f"🔄 Corruption flag set, main recording loop will handle restart for {self.user}")
            
        except Exception as e:
            logger.error(f"Error handling sampler corruption detection for {self.user}: {e}")
    
    def _mark_recording_processed_redis(self):
        """
        Marca la grabación como procesada exitosamente en Redis (para grabaciones directas)
        """
        try:
            # Obtener recording service con Redis
            from services.worker_recording_service import WorkerRecordingService
            recording_service = WorkerRecordingService.get_instance()
            
            if not recording_service or not recording_service.redis_service:
                logger.debug(f"Redis not available for processed mark: {self.user}")
                return
            
            # Buscar recording_id basado en el usuario
            recording_data = recording_service.master_recordings.get(self.user)
            if not recording_data:
                logger.debug(f"No recording data found for processed mark: {self.user}")
                return
            
            recording_id = recording_data.get('recording_id')
            if not recording_id:
                logger.debug(f"No recording_id found for processed mark: {self.user}")
                return
            
            # Usar helper síncrono para llamada async
            from services.redis_helper import RedisAsyncHelper
            
            # Marcar como procesado
            redis_service = recording_service.redis_service
            success = RedisAsyncHelper.run_async_safe(
                redis_service.mark_recording_processed,
                recording_id,
                timeout=3
            )
            
            if success:
                logger.info(f"✅ Marked recording as processed in Redis: {self.user}")
            else:
                logger.warning(f"⚠️ Failed to mark recording as processed in Redis: {self.user}")
            
        except Exception as e:
            logger.error(f"❌ Error marking recording as processed in Redis for {self.user}: {e}")
    
    def _mark_recording_failed_redis(self, reason: str):
        """
        Marca la grabación como fallida en Redis (para grabaciones directas)
        
        Args:
            reason: Razón del fallo
        """
        try:
            # Obtener recording service con Redis
            from services.worker_recording_service import WorkerRecordingService
            recording_service = WorkerRecordingService.get_instance()
            
            if not recording_service or not recording_service.redis_service:
                logger.debug(f"Redis not available for failed mark: {self.user}")
                return
            
            # Buscar recording_id basado en el usuario
            recording_data = recording_service.master_recordings.get(self.user)
            if not recording_data:
                logger.debug(f"No recording data found for failed mark: {self.user}")
                return
            
            recording_id = recording_data.get('recording_id')
            if not recording_id:
                logger.debug(f"No recording_id found for failed mark: {self.user}")
                return
            
            # Usar helper síncrono para llamada async
            from services.redis_helper import RedisAsyncHelper
            
            # Marcar como fallido
            redis_service = recording_service.redis_service
            success = RedisAsyncHelper.run_async_safe(
                redis_service.mark_recording_failed,
                recording_id, reason,
                timeout=3
            )
            
            if success:
                logger.warning(f"❌ Marked recording as failed in Redis: {self.user} (reason: {reason})")
            else:
                logger.warning(f"⚠️ Failed to mark recording as failed in Redis: {self.user}")
            
        except Exception as e:
            logger.error(f"❌ Error marking recording as failed in Redis for {self.user}: {e}")

    def _parse_timestamp_safely(self, line):
        """
        Parsea timestamp de FFmpeg stderr de forma tolerante a diferentes formatos
        
        Args:
            line: Línea de stderr de FFmpeg
            
        Returns:
            tuple: (timestamp_seconds, is_valid) donde timestamp_seconds es float y is_valid es bool
        """
        try:
            # Buscar time= en la línea
            time_match = re.search(r'time=([^\s]+)', line)
            if not time_match:
                return None, False
                
            time_str = time_match.group(1).strip()
            
            # Casos problemáticos
            if time_str in ['N/A', '', '-']:
                return None, False
                
            # Timestamps negativos
            if time_str.startswith('-'):
                return None, False
                
            # Formato estándar: HH:MM:SS.SS
            if re.match(r'\d{2}:\d{2}:\d{2}\.\d{2}', time_str):
                parts = time_str.split(':')
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = float(parts[2])
                total_seconds = hours * 3600 + minutes * 60 + seconds
                return total_seconds, True
                
            return None, False
            
        except Exception as e:
            logger.debug(f"Error parsing timestamp from line: {e}")
            return None, False

    def _trim_gap_from_segment(self, mkv_file_path, gap_start_timestamp):
        """
        Corta un segmento MKV hasta el punto donde empezó el gap, eliminando la porción corrupta
        
        Args:
            mkv_file_path: Ruta al archivo MKV original
            gap_start_timestamp: Timestamp en segundos donde empezó el gap
            
        Returns:
            str: Ruta al archivo trimmed o None si falló
        """
        try:
            import ffmpeg
            
            # Aplicar buffer de seguridad para evitar cortar en medio de frames
            trim_point = max(0, gap_start_timestamp - config.gap_trimming_buffer_seconds)
            
            # Generar nombre del archivo trimmed
            base_path = mkv_file_path.replace('.mkv', '')
            trimmed_path = f"{base_path}_trimmed.mkv"
            
            logger.info(f"Trimming gap from {mkv_file_path}: cutting at {trim_point:.1f}s (gap started at {gap_start_timestamp:.1f}s)")
            
            # Usar FFmpeg para cortar el archivo hasta el punto de gap
            (
                ffmpeg
                .input(mkv_file_path)
                .output(
                    trimmed_path,
                    t=trim_point,  # Duración hasta el punto de corte
                    c='copy',      # Copy streams sin recodificar (rápido)
                    avoid_negative_ts='make_zero',  # Normalizar timestamps
                    y='-y'         # Sobrescribir archivo destino
                )
                .run(quiet=True, overwrite_output=True)
            )
            
            # Verificar que el archivo trimmed se creó correctamente
            if os.path.exists(trimmed_path) and os.path.getsize(trimmed_path) > 0:
                # Obtener información de duración para confirmar
                try:
                    probe = ffmpeg.probe(trimmed_path)
                    trimmed_duration = float(probe['format'].get('duration', 0))
                    original_size = os.path.getsize(mkv_file_path) / (1024 * 1024)  # MB
                    trimmed_size = os.path.getsize(trimmed_path) / (1024 * 1024)   # MB
                    
                    logger.info(f"Gap trimming successful: {trimmed_duration:.1f}s duration, {trimmed_size:.1f}MB (was {original_size:.1f}MB)")
                    
                    # Reemplazar archivo original con el trimmed
                    # Normalize paths for Windows compatibility
                    normalized_mkv_path = os.path.normpath(mkv_file_path)
                    normalized_trimmed_path = os.path.normpath(trimmed_path)
                    os.remove(normalized_mkv_path)
                    os.rename(normalized_trimmed_path, normalized_mkv_path)
                    
                    logger.info(f"Original file replaced with trimmed version: {mkv_file_path}")
                    return mkv_file_path
                    
                except Exception as probe_error:
                    logger.warning(f"Could not probe trimmed file but continuing: {probe_error}")
                    # Aún así proceder con el reemplazo
                    # Normalize paths for Windows compatibility
                    normalized_mkv_path = os.path.normpath(mkv_file_path)
                    normalized_trimmed_path = os.path.normpath(trimmed_path)
                    os.remove(normalized_mkv_path)
                    os.rename(normalized_trimmed_path, normalized_mkv_path)
                    return mkv_file_path
            else:
                logger.error(f"Gap trimming failed: output file not created or empty")
                # Limpiar archivo trimmed fallido si existe
                normalized_trimmed_path = os.path.normpath(trimmed_path)
                if os.path.exists(normalized_trimmed_path):
                    os.remove(normalized_trimmed_path)
                return None
                
        except ffmpeg.Error as e:
            logger.error(f"FFmpeg error during gap trimming: {e.stderr.decode() if hasattr(e, 'stderr') else str(e)}")
            # Limpiar archivo trimmed parcial si existe
            trimmed_path = mkv_file_path.replace('.mkv', '_trimmed.mkv')
            if os.path.exists(trimmed_path):
                try:
                    os.remove(trimmed_path)
                except:
                    pass
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error during gap trimming: {e}")
            return None