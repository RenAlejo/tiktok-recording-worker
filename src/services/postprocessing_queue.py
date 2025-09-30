"""
Cola de post-procesamiento asíncrona para grabaciones con múltiples segmentos.
Maneja la validación de corrupción y concatenación de forma paralela y no bloqueante.
"""

import asyncio
import time
import os
import threading
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

from utils.logger_manager import logger
from config.env_config import config
from utils.corrupted_segment_detector import CorruptedSegmentDetector


class ProcessingStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    VALIDATING = "validating"
    CONCATENATING = "concatenating"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class RecordingJob:
    """Representa un trabajo de post-procesamiento en la cola"""
    job_id: str
    user: str
    recording_parts: List[str]
    output_info: Dict[str, Any]
    timestamp: float
    status: ProcessingStatus = ProcessingStatus.PENDING
    retry_count: int = 0
    error_message: Optional[str] = None
    processed_parts: List[str] = None
    final_output: Optional[str] = None


class PostProcessingQueue:
    """
    Cola asíncrona para post-procesamiento de grabaciones con múltiples segmentos.
    
    Características:
    - Procesamiento paralelo con múltiples workers
    - Validación de corrupción por segmento
    - Concatenación inteligente
    - Manejo de errores y reintentos
    - Monitoreo de estado en tiempo real
    """
    
    def __init__(self):
        self.queue = asyncio.Queue(maxsize=config.max_queue_size)
        self.workers_count = config.postprocessing_workers
        self.processing_tasks = []
        self.jobs: Dict[str, RecordingJob] = {}
        self.stats = {
            'total_jobs': 0,
            'completed_jobs': 0,
            'failed_jobs': 0,
            'processing_jobs': 0
        }
        self._running = False
        self._lock = threading.Lock()
        
        logger.info(f"🏭 PostProcessingQueue initialized with {self.workers_count} workers, max queue size: {config.max_queue_size}")
    
    async def start(self):
        """Inicia los workers de la cola"""
        if self._running:
            return
            
        self._running = True
        
        # Crear workers
        for i in range(self.workers_count):
            task = asyncio.create_task(self._worker(f"Worker-{i+1}"))
            self.processing_tasks.append(task)
            
        logger.info(f"🚀 PostProcessingQueue started with {self.workers_count} workers")
    
    async def stop(self):
        """Detiene la cola y espera a que terminen los trabajos actuales"""
        if not self._running:
            return
            
        self._running = False
        
        # Esperar a que terminen los workers
        for task in self.processing_tasks:
            task.cancel()
        
        await asyncio.gather(*self.processing_tasks, return_exceptions=True)
        self.processing_tasks.clear()
        
        logger.info("🛑 PostProcessingQueue stopped")
    
    async def add_recording_for_postprocessing(self, user: str, recording_parts: List[str], 
                                             output_info: Dict[str, Any]) -> Optional[str]:
        """
        Añade una grabación completa a la cola de post-procesamiento
        
        Args:
            user: Nombre del usuario
            recording_parts: Lista de archivos de partes de la grabación
            output_info: Información de salida (telegram_chat_id, fragment_number, etc.)
            
        Returns:
            job_id si se añadió a la cola, None si se procesó inmediatamente
        """
        if len(recording_parts) >= config.min_segments_for_queue and config.enable_postprocessing_queue:
            # Enviar a cola de post-procesamiento
            job_id = f"{user}_{int(time.time())}"
            
            job = RecordingJob(
                job_id=job_id,
                user=user,
                recording_parts=recording_parts.copy(),
                output_info=output_info,
                timestamp=time.time()
            )
            
            try:
                await self.queue.put(job)
                self.jobs[job_id] = job
                self.stats['total_jobs'] += 1
                
                logger.info(f"📥 Added {user} to post-processing queue ({len(recording_parts)} parts) - Job ID: {job_id}")
                return job_id
                
            except asyncio.QueueFull:
                logger.error(f"❌ Post-processing queue is full, processing {user} immediately")
                await self._process_immediately(user, recording_parts, output_info)
                return None
        else:
            # Proceso inmediato para grabaciones pequeñas
            logger.info(f"⚡ Processing {user} immediately ({len(recording_parts)} parts < {config.min_segments_for_queue} threshold)")
            await self._process_immediately(user, recording_parts, output_info)
            return None
    
    async def _worker(self, worker_name: str):
        """Worker que procesa grabaciones de la cola"""
        logger.info(f"👷 {worker_name} started")
        
        while self._running:
            try:
                # Obtener trabajo de la cola con timeout
                job = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                
                logger.info(f"👷 {worker_name} processing job {job.job_id} for {job.user}")
                self.stats['processing_jobs'] += 1
                
                # Procesar el trabajo
                await self._process_recording_with_corruption_validation(job, worker_name)
                
                # Marcar como completado en la cola
                self.queue.task_done()
                self.stats['processing_jobs'] -= 1
                
            except asyncio.TimeoutError:
                # Normal, continúa el loop
                continue
            except asyncio.CancelledError:
                logger.info(f"👷 {worker_name} cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in {worker_name}: {e}")
                self.stats['processing_jobs'] -= 1
        
        logger.info(f"👷 {worker_name} stopped")
    
    async def _process_recording_with_corruption_validation(self, job: RecordingJob, worker_name: str):
        """
        Procesa una grabación completa con validación de corrupción por segmento
        """
        try:
            job.status = ProcessingStatus.VALIDATING
            timeout_seconds = config.postprocessing_timeout_minutes * 60
            
            logger.info(f"🔍 [{worker_name}] Starting validation for {job.user} ({len(job.recording_parts)} parts)")
            
            # PASO 1: Detección rápida de segmentos corruptos/audio-only
            logger.debug(f"[{worker_name}] Pre-filtering corrupted segments")
            pre_filter_result = CorruptedSegmentDetector.filter_valid_segments(job.recording_parts, f"[{worker_name}]")
            
            if pre_filter_result['filter_stats']['corrupted_found'] > 0:
                logger.info(f"🗑️ [{worker_name}] Pre-filtered {pre_filter_result['filter_stats']['corrupted_found']} corrupted segments")
            
            # Usar solo los segmentos que pasaron el pre-filtro
            remaining_parts = pre_filter_result['valid_segments']
            
            if not remaining_parts:
                logger.error(f"❌ [{worker_name}] All parts filtered as corrupted for {job.user}")
                await self._cleanup_failed_recording(job, worker_name)
                raise Exception(f"No valid parts remaining after pre-filtering for {job.user}")
            
            # PASO 2: Validación de corrupción tradicional para archivos restantes
            logger.debug(f"[{worker_name}] Starting traditional corruption analysis for {len(remaining_parts)} remaining parts")
            valid_parts = []
            corrupted_parts = []
            total_size_filtered = 0
            
            for i, part_file in enumerate(remaining_parts):
                if not os.path.exists(part_file):
                    logger.warning(f"❌ [{worker_name}] Missing part {i+1}: {part_file}")
                    continue
                
                part_size_mb = os.path.getsize(part_file) / (1024 * 1024)
                
                # Aplicar validación de corrupción si el archivo está dentro del límite de tamaño
                if part_size_mb <= config.queue_corruption_threshold_mb:
                    corruption_result = await self._analyze_corruption_async(part_file, worker_name)
                    
                    if corruption_result['should_discard']:
                        corrupted_parts.append(part_file)
                        total_size_filtered += part_size_mb
                        logger.warning(f"🗑️ [{worker_name}] Part {i+1} corrupted ({corruption_result['error_percentage']:.1f}% errors), discarding: {os.path.basename(part_file)}")
                        
                        # Eliminar archivo corrupto del disco
                        try:
                            os.remove(part_file)
                            logger.info(f"✅ [{worker_name}] Deleted corrupted file: {os.path.basename(part_file)}")
                        except Exception as e:
                            logger.error(f"Error deleting corrupted file {part_file}: {e}")
                    else:
                        valid_parts.append(part_file)
                        logger.info(f"✅ [{worker_name}] Part {i+1} valid ({corruption_result['error_percentage']:.1f}% errors): {os.path.basename(part_file)}")
                else:
                    # Archivo demasiado grande, asumir válido
                    valid_parts.append(part_file)
                    logger.info(f"📏 [{worker_name}] Part {i+1} too large ({part_size_mb:.1f}MB), skipping validation: {os.path.basename(part_file)}")
            
            job.processed_parts = valid_parts
            
            if corrupted_parts:
                logger.info(f"🧹 [{worker_name}] Filtered {len(corrupted_parts)} corrupted parts ({total_size_filtered:.1f}MB) for {job.user}")
            
            if not valid_parts:
                logger.error(f"❌ [{worker_name}] All parts corrupted for {job.user} - deleting entire recording")
                
                # Eliminar cualquier archivo restante de la grabación completa
                await self._cleanup_failed_recording(job, worker_name)
                
                raise Exception(f"No valid parts remaining after corruption filtering for {job.user}")
            
            job.status = ProcessingStatus.CONCATENATING
            
            # MARCAR EN REDIS como procesando
            await self._mark_recording_processing_redis(job)
            
            # Continuar con concatenación y procesamiento final
            await self._finalize_recording_processing(job, worker_name)
            
            job.status = ProcessingStatus.COMPLETED
            self.stats['completed_jobs'] += 1
            logger.info(f"✅ [{worker_name}] Job {job.job_id} completed successfully for {job.user}")
            
            # MARCAR EN REDIS como procesado exitosamente
            await self._mark_recording_processed_redis(job)
            
        except asyncio.TimeoutError:
            error_msg = f"Processing timeout ({config.postprocessing_timeout_minutes} minutes) for {job.user}"
            logger.error(f"⏰ [{worker_name}] {error_msg}")
            job.status = ProcessingStatus.FAILED
            job.error_message = error_msg
            self.stats['failed_jobs'] += 1
            
            # MARCAR EN REDIS como fallido
            await self._mark_recording_failed_redis(job, error_msg)
            
        except Exception as e:
            error_msg = f"Processing error for {job.user}: {e}"
            logger.error(f"❌ [{worker_name}] {error_msg}")
            job.status = ProcessingStatus.FAILED
            job.error_message = error_msg
            self.stats['failed_jobs'] += 1
            
            # MARCAR EN REDIS como fallido
            await self._mark_recording_failed_redis(job, error_msg)
    
    async def _analyze_corruption_async(self, video_path: str, worker_name: str) -> Dict[str, Any]:
        """
        Analiza corrupción de video de forma asíncrona
        """
        loop = asyncio.get_event_loop()
        
        # Ejecutar el análisis en un thread separado para no bloquear el worker
        return await loop.run_in_executor(None, self._analyze_corruption_sync, video_path, worker_name)
    
    def _analyze_corruption_sync(self, video_path: str, worker_name: str) -> Dict[str, Any]:
        """
        Análisis síncrono de corrupción (ejecutado en thread separado)
        """
        import subprocess
        import re
        
        try:
            # Análisis de corrupción usando FFmpeg
            comando = [
                'ffmpeg',
                '-v', 'error',
                '-i', video_path,
                '-f', 'null', '-'
            ]
            
            result = subprocess.run(
                comando,
                capture_output=True,
                text=True,
                timeout=60  # 1 minuto timeout por archivo
            )
            
            stderr_output = result.stderr
            errores_criticos = 0
            total_frames = 0
            
            # Patrones de errores críticos
            patrones_errores = [
                r'error while decoding',
                r'unavailable for requested'
            ]
            
            for linea in stderr_output.split('\n'):
                linea = linea.strip()
                if not linea:
                    continue
                    
                # Contar errores críticos
                for patron in patrones_errores:
                    if re.search(patron, linea, re.IGNORECASE):
                        errores_criticos += 1
                        break
                
                # Extraer información de frames
                if 'frame=' in linea:
                    match = re.search(r'frame=\s*(\d+)', linea)
                    if match:
                        total_frames = max(total_frames, int(match.group(1)))
            
            # Si no se obtuvieron frames, usar ffprobe como fallback
            if total_frames == 0:
                try:
                    probe_cmd = [
                        'ffprobe', 
                        '-v', 'quiet',
                        '-select_streams', 'v:0',
                        '-count_frames',
                        '-show_entries', 'stream=nb_frames',
                        '-of', 'csv=p=0',
                        video_path
                    ]
                    
                    probe_result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=30)
                    if probe_result.returncode == 0 and probe_result.stdout.strip():
                        total_frames = int(probe_result.stdout.strip())
                except Exception:
                    total_frames = max(1, errores_criticos * 10)
            
            # Calcular porcentaje de errores
            if total_frames > 0:
                porcentaje_errores = (errores_criticos / total_frames) * 100
            else:
                porcentaje_errores = 0
            
            # Umbral de descarte del 50%
            should_discard = porcentaje_errores >= 30.0
            
            return {
                'should_discard': should_discard,
                'error_percentage': porcentaje_errores,
                'critical_errors': errores_criticos,
                'total_frames': total_frames,
                'threshold': 30.0
            }
            
        except subprocess.TimeoutExpired:
            logger.warning(f"[{worker_name}] Corruption analysis timeout for {os.path.basename(video_path)}")
            return {
                'should_discard': False,
                'error_percentage': 0.0,
                'critical_errors': 0,
                'total_frames': 0,
                'threshold': 30.0
            }
        except Exception as e:
            logger.error(f"[{worker_name}] Error in corruption analysis for {os.path.basename(video_path)}: {e}")
            return {
                'should_discard': False,
                'error_percentage': 0.0,
                'critical_errors': 0,
                'total_frames': 0,
                'threshold': 50.0
            }
    
    async def _finalize_recording_processing(self, job: RecordingJob, worker_name: str):
        """
        Finaliza el procesamiento de la grabación (concatenación y upload)
        """
        from utils.video_management import VideoManagement
        import tempfile
        import ffmpeg
        
        valid_parts = job.processed_parts
        
        if len(valid_parts) > 1:
            job.status = ProcessingStatus.CONCATENATING
            logger.info(f"🔗 [{worker_name}] Concatenating {len(valid_parts)} valid parts for {job.user}")
            
            # Crear archivo de lista temporal para FFmpeg
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
                for part in valid_parts:
                    escaped_path = part.replace('\\', '/').replace("'", "\\'")
                    f.write(f"file '{escaped_path}'\n")
                list_file = f.name
            
            try:
                # Generar nombre del archivo concatenado
                first_part = valid_parts[0]
                base_name = first_part.replace('.mp4', '').split('_cont')[0] if '_cont' in first_part else first_part.replace('.mp4', '')
                final_output = f"{base_name}_COMPLETE.mp4"
                
                # Concatenar usando FFmpeg
                ffmpeg.input(list_file, format='concat', safe=0).output(
                    final_output,
                    c='copy',
                    avoid_negative_ts='make_zero',
                    movflags='+faststart'
                ).run(quiet=True, overwrite_output=True)
                
                # Verificar concatenación exitosa
                if os.path.exists(final_output) and os.path.getsize(final_output) > 1024:
                    final_size = os.path.getsize(final_output) / (1024*1024)
                    logger.info(f"✅ [{worker_name}] Concatenation successful: {os.path.basename(final_output)} ({final_size:.1f}MB)")
                    
                    # Limpiar partes individuales
                    for part in valid_parts:
                        try:
                            if os.path.exists(part):
                                os.remove(part)
                        except Exception as e:
                            logger.warning(f"Could not remove part {part}: {e}")
                    
                    job.final_output = final_output
                else:
                    raise Exception(f"Concatenation failed - output file invalid")
                
            finally:
                # Limpiar archivo de lista temporal
                try:
                    os.remove(list_file)
                except Exception:
                    pass
        
        elif len(valid_parts) == 1:
            # Una sola parte válida
            job.final_output = valid_parts[0]
            logger.info(f"📁 [{worker_name}] Single valid part for {job.user}: {os.path.basename(job.final_output)}")
        else:
            raise Exception("No valid parts to process")
        
        # Procesar para upload a Telegram
        await self._process_for_telegram(job, worker_name)
    
    async def _process_for_telegram(self, job: RecordingJob, worker_name: str):
        """
        Procesa el video final para upload a Telegram
        """
        job.status = ProcessingStatus.UPLOADING
        
        # Importar aquí para evitar circular imports
        from upload.upload_manager import UploadManager
        from utils.video_collage import VideoCollageGenerator
        
        final_video = job.final_output
        output_info = job.output_info
        
        logger.info(f"📤 [{worker_name}] Processing for Telegram upload: {job.user}")
        
        # Validar MP4
        if not self._validate_mp4_file(final_video):
            raise Exception(f"Final MP4 file is corrupted: {final_video}")
        
        # Generar collage y thumbnail si está habilitado
        collage_path = None
        thumbnail_path = None
        
        if config.enable_video_collage:
            try:
                collage_generator = VideoCollageGenerator()
                
                # Generar rutas para collage y thumbnail
                video_dir = os.path.dirname(final_video)
                video_name = os.path.splitext(os.path.basename(final_video))[0]
                collage_output_path = os.path.join(video_dir, f"{video_name}_collage.jpg")
                
                # Generar collage
                if collage_generator.generate_collage(final_video, collage_output_path):
                    collage_path = collage_output_path
                    logger.info(f"📸 [{worker_name}] Video collage generated: {collage_path}")
                
                # Generar thumbnail (primera captura del video)
                thumbnail_output_path = os.path.join(video_dir, f"{video_name}_thumb.jpg")
                if self._generate_video_thumbnail(final_video, thumbnail_output_path, worker_name):
                    thumbnail_path = thumbnail_output_path
                    logger.info(f"🖼️ [{worker_name}] Video thumbnail generated: {thumbnail_path}")
                    
            except Exception as e:
                logger.warning(f"Failed to generate collage/thumbnail for {job.user}: {e}")
        
        # Añadir a cola de upload
        upload_manager = UploadManager()
        is_fragmented_value = output_info.get('is_fragmented', False)
        fragment_number_value = output_info.get('fragment_number', 1)
        
        
        await upload_manager.add_upload_task(
            final_video,
            output_info.get('telegram_chat_id'),
            job.user,
            fragment_number_value,
            is_fragmented_value,
            collage_path=collage_path,
            thumbnail_path=thumbnail_path
        )
        
        logger.info(f"✅ [{worker_name}] Upload task added for {job.user}")
        
        # NOTA: La limpieza de la carpeta se hace ahora en UploadManager después del upload exitoso
    
    def _generate_video_thumbnail(self, video_path: str, thumbnail_path: str, worker_name: str) -> bool:
        """
        Genera un thumbnail del video (primera captura)
        """
        try:
            import ffmpeg
            
            # Extraer primera captura del video
            (
                ffmpeg
                .input(video_path, ss=1)  # Tomar captura en el segundo 1
                .output(thumbnail_path, vframes=1, format='mjpeg')
                .overwrite_output()
                .run(quiet=True)
            )
            
            if os.path.exists(thumbnail_path) and os.path.getsize(thumbnail_path) > 0:
                logger.debug(f"[{worker_name}] Thumbnail generated successfully: {thumbnail_path}")
                return True
            else:
                logger.warning(f"[{worker_name}] Thumbnail generation failed: empty or missing file")
                return False
                
        except Exception as e:
            logger.error(f"[{worker_name}] Error generating thumbnail: {e}")
            return False
    
    def _validate_mp4_file(self, mp4_path: str) -> bool:
        """
        Validación básica de archivo MP4
        """
        try:
            import ffmpeg
            
            if not os.path.exists(mp4_path):
                return False
            
            file_size = os.path.getsize(mp4_path)
            if file_size < 1024:  # < 1KB
                return False
            
            # Verificar con ffprobe
            probe = ffmpeg.probe(mp4_path)
            video_streams = [stream for stream in probe['streams'] if stream['codec_type'] == 'video']
            
            return len(video_streams) > 0
            
        except Exception:
            return False
    
    async def _process_immediately(self, user: str, recording_parts: List[str], output_info: Dict[str, Any]):
        """
        Procesa una grabación inmediatamente (sin cola) - INCLUYE INTEGRACIÓN REDIS
        """
        logger.info(f"⚡ Processing {user} immediately - bypassing queue")
        
        # Crear un job temporal para reutilizar la lógica existente
        temp_job = RecordingJob(
            job_id=f"immediate_{user}_{int(time.time())}",
            user=user,
            recording_parts=recording_parts,
            output_info=output_info,
            timestamp=time.time(),
            processed_parts=recording_parts.copy()  # Asumir todas válidas para proceso inmediato
        )
        
        try:
            # MARCAR EN REDIS como procesando (para grabaciones con pocos segmentos)
            temp_job.status = ProcessingStatus.PROCESSING
            await self._mark_recording_processing_redis(temp_job)
            
            # Procesar inmediatamente
            await self._finalize_recording_processing(temp_job, "Immediate")
            
            # MARCAR EN REDIS como procesado (para grabaciones con pocos segmentos)
            temp_job.status = ProcessingStatus.COMPLETED
            await self._mark_recording_processed_redis(temp_job)
            
            logger.info(f"✅ Immediate processing completed for {user}")
            
        except Exception as e:
            # MARCAR EN REDIS como fallido (para grabaciones con pocos segmentos)
            error_msg = f"Immediate processing failed: {e}"
            temp_job.status = ProcessingStatus.FAILED
            await self._mark_recording_failed_redis(temp_job, error_msg)
            
            logger.error(f"❌ Immediate processing failed for {user}: {e}")
            raise
    
    def get_queue_status(self) -> Dict[str, Any]:
        """
        Obtiene el estado actual de la cola
        """
        with self._lock:
            return {
                'queue_size': self.queue.qsize(),
                'max_queue_size': self.queue.maxsize,
                'workers_count': self.workers_count,
                'running': self._running,
                'stats': self.stats.copy(),
                'active_jobs': len([job for job in self.jobs.values() if job.status == ProcessingStatus.PROCESSING])
            }
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene el estado de un trabajo específico
        """
        job = self.jobs.get(job_id)
        if not job:
            return None
        
        return {
            'job_id': job.job_id,
            'user': job.user,
            'status': job.status.value,
            'parts_count': len(job.recording_parts),
            'processed_parts_count': len(job.processed_parts) if job.processed_parts else 0,
            'timestamp': job.timestamp,
            'retry_count': job.retry_count,
            'error_message': job.error_message,
            'final_output': job.final_output
        }

    async def _cleanup_failed_recording(self, job: RecordingJob, worker_name: str):
        """
        Limpia completamente una grabación que falló el post-procesamiento
        Elimina todos los archivos restantes y limpia la carpeta de grabación
        
        Args:
            job: El trabajo de grabación fallido
            worker_name: Nombre del worker para logging
        """
        try:
            logger.warning(f"🗑️ [{worker_name}] Starting cleanup of failed recording for {job.user}")
            
            # Recopilar todos los archivos que necesitan limpieza
            all_files_to_clean = set()
            
            # Agregar archivos originales de la grabación
            for part_file in job.recording_parts:
                if os.path.exists(part_file):
                    all_files_to_clean.add(part_file)
            
            # Buscar archivos adicionales en la carpeta de grabación que puedan haber quedado
            if job.recording_parts:
                first_file = job.recording_parts[0]
                recording_folder = os.path.dirname(first_file)
                
                if os.path.exists(recording_folder):
                    try:
                        # Buscar archivos de video que pertenezcan a esta grabación
                        for file in os.listdir(recording_folder):
                            file_path = os.path.join(recording_folder, file)
                            if os.path.isfile(file_path):
                                # Verificar si es un archivo de video de esta grabación
                                if any(ext in file.lower() for ext in ['.mp4', '.mkv', '.avi', '.ts', '.flv']):
                                    # Verificar si pertenece a esta grabación por patrón de nombre
                                    base_filename = os.path.basename(first_file)
                                    recording_prefix = base_filename.split('_cont')[0] if '_cont' in base_filename else base_filename.split('.')[0]
                                    
                                    if recording_prefix in file:
                                        all_files_to_clean.add(file_path)
                    except Exception as e:
                        logger.debug(f"[{worker_name}] Could not scan recording folder for cleanup: {e}")
            
            # Eliminar todos los archivos identificados
            files_deleted = 0
            total_size_cleaned = 0
            
            for part_file in all_files_to_clean:
                try:
                    if os.path.exists(part_file):
                        file_size = os.path.getsize(part_file)
                        os.remove(part_file)
                        files_deleted += 1
                        total_size_cleaned += file_size
                        logger.debug(f"🗑️ [{worker_name}] Deleted {os.path.basename(part_file)} ({file_size/1024/1024:.1f}MB)")
                except Exception as e:
                    logger.error(f"❌ [{worker_name}] Could not delete {part_file}: {e}")
            
            # Si se eliminaron archivos, limpiar la carpeta de grabación también
            if files_deleted > 0:
                logger.info(f"🧹 [{worker_name}] Cleaned up {files_deleted} files ({total_size_cleaned/1024/1024:.1f}MB) for failed recording: {job.user}")
                
                # Intentar limpiar la carpeta de grabación si está vacía
                try:
                    if job.recording_parts:
                        first_file = job.recording_parts[0]
                        recording_folder = os.path.dirname(first_file)
                        
                        # Solo intentar eliminar si es una carpeta organizada (no el directorio base)
                        if recording_folder and recording_folder != "/home/admin/recordings2":
                            folder_contents = os.listdir(recording_folder)
                            if not folder_contents:
                                os.rmdir(recording_folder)
                                logger.info(f"🗑️ [{worker_name}] Cleaned up empty recording folder: {recording_folder}")
                            else:
                                logger.debug(f"📁 [{worker_name}] Recording folder not empty, keeping: {recording_folder}")
                except Exception as folder_error:
                    logger.debug(f"Could not cleanup recording folder: {folder_error}")
            else:
                logger.debug(f"🔍 [{worker_name}] No files found to cleanup for {job.user}")
                
        except Exception as e:
            logger.error(f"❌ [{worker_name}] Error during failed recording cleanup: {e}")
    
    async def _mark_recording_processing_redis(self, job: RecordingJob):
        """
        Marca la grabación como procesando en Redis
        """
        try:
            # Obtener recording service con Redis
            from services.worker_recording_service import WorkerRecordingService
            recording_service = WorkerRecordingService.get_instance()
            
            if not recording_service or not recording_service.redis_service:
                logger.debug(f"Redis not available for processing mark: {job.user}")
                return
            
            # Buscar recording_id basado en el usuario
            recording_data = recording_service.master_recordings.get(job.user)
            if not recording_data:
                logger.debug(f"No recording data found for processing mark: {job.user}")
                return
            
            recording_id = recording_data.get('recording_id')
            if not recording_id:
                logger.debug(f"No recording_id found for processing mark: {job.user}")
                return
            
            # Marcar como procesando
            redis_service = recording_service.redis_service
            await redis_service.mark_recording_processing(recording_id)
            logger.debug(f"🔄 Marked recording as processing in Redis: {job.user}")
            
        except Exception as e:
            logger.error(f"❌ Error marking recording as processing in Redis for {job.user}: {e}")
    
    async def _mark_recording_processed_redis(self, job: RecordingJob):
        """
        Marca la grabación como procesada exitosamente en Redis
        """
        try:
            # Obtener recording service con Redis
            from services.worker_recording_service import WorkerRecordingService
            recording_service = WorkerRecordingService.get_instance()
            
            if not recording_service or not recording_service.redis_service:
                logger.debug(f"Redis not available for processed mark: {job.user}")
                return
            
            # Buscar recording_id basado en el usuario
            recording_data = recording_service.master_recordings.get(job.user)
            if not recording_data:
                logger.debug(f"No recording data found for processed mark: {job.user}")
                return
            
            recording_id = recording_data.get('recording_id')
            if not recording_id:
                logger.debug(f"No recording_id found for processed mark: {job.user}")
                return
            
            # Marcar como procesado
            redis_service = recording_service.redis_service
            await redis_service.mark_recording_processed(recording_id)
            logger.info(f"✅ Marked recording as processed in Redis: {job.user}")
            
        except Exception as e:
            logger.error(f"❌ Error marking recording as processed in Redis for {job.user}: {e}")
    
    async def _mark_recording_failed_redis(self, job: RecordingJob, reason: str):
        """
        Marca la grabación como fallida en Redis
        """
        try:
            # Obtener recording service con Redis
            from services.worker_recording_service import WorkerRecordingService
            recording_service = WorkerRecordingService.get_instance()
            
            if not recording_service or not recording_service.redis_service:
                logger.debug(f"Redis not available for failed mark: {job.user}")
                return
            
            # Buscar recording_id basado en el usuario
            recording_data = recording_service.master_recordings.get(job.user)
            if not recording_data:
                logger.debug(f"No recording data found for failed mark: {job.user}")
                return
            
            recording_id = recording_data.get('recording_id')
            if not recording_id:
                logger.debug(f"No recording_id found for failed mark: {job.user}")
                return
            
            # Marcar como fallido
            redis_service = recording_service.redis_service
            await redis_service.mark_recording_failed(recording_id, reason)
            logger.warning(f"❌ Marked recording as failed in Redis: {job.user} (reason: {reason})")
            
        except Exception as e:
            logger.error(f"❌ Error marking recording as failed in Redis for {job.user}: {e}")


# Instancia global de la cola
postprocessing_queue = PostProcessingQueue()