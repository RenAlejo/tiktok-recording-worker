"""
Muestreador de video en tiempo real para detectar corrupción silenciosa durante la grabación.
Extrae y analiza muestras del video cada cierto intervalo para detectar corrupción visual
que FFmpeg no reporta en tiempo real (bandas grises, colores anómalos, etc.).
"""

import os
import time
import threading
import subprocess
import re
import tempfile
import errno
from typing import Dict, Any, Optional, Callable
from pathlib import Path
from contextlib import contextmanager

from utils.logger_manager import logger
from config.env_config import config


@contextmanager
def managed_subprocess(*args, **kwargs):
    """
    Context manager que garantiza cleanup automático de procesos subprocess
    Previene fugas de file descriptors cerrando procesos inmediatamente después del uso
    """
    process = None
    try:
        process = subprocess.Popen(*args, **kwargs)
        yield process
    finally:
        if process:
            try:
                # Intentar terminación limpia primero
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        # Force kill si no responde
                        process.kill()
                        process.wait(timeout=1)
                        
                # Cerrar explícitamente todos los pipes para liberar file descriptors
                if hasattr(process, 'stdin') and process.stdin:
                    process.stdin.close()
                if hasattr(process, 'stdout') and process.stdout:
                    process.stdout.close()
                if hasattr(process, 'stderr') and process.stderr:
                    process.stderr.close()
                    
            except Exception as e:
                logger.debug(f"Error during subprocess cleanup: {e}")


def retry_on_resource_exhaustion(max_retries: int = 3, delay: float = 1.0):
    """
    Decorator para reintentar operaciones cuando se agotan los file descriptors
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (OSError, subprocess.SubprocessError) as e:
                    last_exception = e
                    # Verificar si es error de "Too many open files"
                    if hasattr(e, 'errno') and e.errno == errno.EMFILE:
                        logger.warning(f"🔍 Resource exhaustion detected (attempt {attempt + 1}/{max_retries}): {e}")
                        if attempt < max_retries - 1:
                            time.sleep(delay * (attempt + 1))  # Backoff exponencial
                            continue
                    raise e
                except Exception as e:
                    last_exception = e
                    if "Too many open files" in str(e):
                        logger.warning(f"🔍 File descriptor limit reached (attempt {attempt + 1}/{max_retries}): {e}")
                        if attempt < max_retries - 1:
                            time.sleep(delay * (attempt + 1))
                            continue
                    raise e
            raise last_exception
        return wrapper
    return decorator


class RealtimeVideoSampler:
    """
    Muestreador de video en tiempo real que detecta corrupción silenciosa
    extrayendo y analizando pequeñas muestras del archivo de grabación activo.
    """
    
    def __init__(self, user: str, on_corruption_detected: Callable[[], None]):
        """
        Args:
            user: Nombre del usuario para identificar las muestras
            on_corruption_detected: Callback a ejecutar cuando se detecta corrupción
                                  (debe cortar segmento e iniciar nuevo)
        """
        
        self.user = user
        self.on_corruption_detected = on_corruption_detected
        self.current_file_path: Optional[str] = None
        self.recording_start_time: Optional[float] = None
        self.last_sample_time: Optional[float] = None
        self.sampling_thread: Optional[threading.Thread] = None
        self.is_sampling_active = False
        self._stop_event = threading.Event()
        
        logger.debug(f"RealtimeVideoSampler initialized for {self.user} - interval: {config.sampling_interval_seconds}s, threshold: {config.sample_corruption_threshold_percent}%")
    
    def start_sampling(self, file_path: str):
        """
        Inicia el muestreo en tiempo real para un archivo de grabación
        
        Args:
            file_path: Ruta al archivo de video que se está grabando
        """
        if not config.enable_realtime_sampling:
            logger.debug("Real-time sampling disabled by configuration")
            return
            
        self.stop_sampling()  # Detener muestreo anterior si existe
        
        self.current_file_path = file_path
        self.recording_start_time = time.time()
        self.last_sample_time = None
        self.is_sampling_active = True
        self._stop_event.clear()
        
        # Iniciar thread de muestreo
        self.sampling_thread = threading.Thread(
            target=self._sampling_loop,
            name=f"VideoSampler-{Path(file_path).stem}",
            daemon=True
        )
        self.sampling_thread.start()
        
        logger.info(f"Started real-time sampling for: {os.path.basename(file_path)}")
    
    def stop_sampling(self):
        """Detiene el muestreo en tiempo real"""
        if self.is_sampling_active:
            self.is_sampling_active = False
            self._stop_event.set()
            
            if self.sampling_thread and self.sampling_thread.is_alive():
                self.sampling_thread.join(timeout=5)
                if self.sampling_thread.is_alive():
                    logger.warning("🔍 Sampling thread did not terminate gracefully")
            
            logger.debug("🔍 Real-time sampling stopped")
        
        self.current_file_path = None
        self.recording_start_time = None
        self.last_sample_time = None
    
    def _sampling_loop(self):
        """Loop principal de muestreo ejecutado en thread separado"""
        logger.debug(f"Sampling loop started for {os.path.basename(self.current_file_path)}")
        
        try:
            while self.is_sampling_active and not self._stop_event.is_set():
                # Esperar hasta el próximo intervalo
                if self._stop_event.wait(timeout=config.sampling_interval_seconds):
                    break  # Stop event triggered
                
                if not self.is_sampling_active:
                    break
                
                # Verificar si es momento de hacer muestreo
                if self._should_perform_sample():
                    try:
                        self._perform_corruption_sample()
                    except Exception as e:
                        logger.error(f"Error during sampling: {e}")
                        # No detener el loop por errores individuales
                        continue
                        
        except Exception as e:
            logger.error(f"Critical error in sampling loop: {e}")
        finally:
            logger.debug(f"Sampling loop ended for {os.path.basename(self.current_file_path) if self.current_file_path else 'unknown'}")
    
    def _should_perform_sample(self) -> bool:
        """
        Determina si es momento de realizar una muestra
        """
        if not self.current_file_path or not os.path.exists(self.current_file_path):
            return False
        
        # Verificar tiempo mínimo de grabación antes del primer muestreo
        recording_duration = time.time() - self.recording_start_time
        if recording_duration < config.min_recording_duration_before_sampling_seconds:
            return False
        
        # Verificar que el archivo tenga suficiente contenido para muestrear
        try:
            file_size = os.path.getsize(self.current_file_path)
            if file_size < 1024 * 1024:  # < 1MB, probablemente muy pequeño
                return False
        except OSError:
            return False
        return True
    
    def _perform_corruption_sample(self):
        """
        Extrae una muestra del final del video y la analiza en busca de corrupción
        """
        if not self.current_file_path:
            return
            
        sample_start_time = time.time()
        sample_file = None
        
        try:
            # Crear carpeta temp en OUTPUT_DIRECTORY si no existe
            temp_dir = os.path.join(config.output_directory, "temp")
            os.makedirs(temp_dir, exist_ok=True)
            
            # Limpiar muestras antiguas del usuario (solo si hay más de 3)
            self._cleanup_old_samples(temp_dir)
            
            # Crear archivo temporal para la muestra en la carpeta configurada
            timestamp = int(time.time())
            sample_filename = f"sample_{self.user}_{timestamp}.mp4"
            sample_file = os.path.join(temp_dir, sample_filename)
            
            # Extraer últimos X segundos del archivo usando FFmpeg
            if not self._extract_video_sample(self.current_file_path, sample_file):
                logger.warning("🔍 Failed to extract sample, skipping analysis")
                # Limpiar archivo temporal parcial si existe
                if os.path.exists(sample_file):
                    try:
                        os.remove(sample_file)
                    except Exception:
                        pass
                return
            
            # Analizar corrupción en la muestra usando la misma lógica del post-processing
            corruption_result = self._analyze_sample_corruption(sample_file)
            
            sample_duration = time.time() - sample_start_time
            
            # Log resultado del análisis
            logger.info(f"Real-time sample analysis completed in {sample_duration:.1f}s - corruption: {corruption_result['error_percentage']:.1f}% (threshold: {config.sample_corruption_threshold_percent}%)")
            
            # Verificar si la corrupción excede el umbral
            if corruption_result['should_discard']:
                logger.warning(f"🚨 CORRUPTION DETECTED in real-time sample: {corruption_result['error_percentage']:.1f}% > {config.sample_corruption_threshold_percent}%")
                logger.warning(f"🚨 Triggering segment restart due to silent corruption detection")
                
                # Activar callback para cortar segmento actual
                self.on_corruption_detected()
            else:
                logger.info(f"✅ Sample clean: {corruption_result['error_percentage']:.1f}% corruption (threshold: {config.sample_corruption_threshold_percent}%)")
            
            self.last_sample_time = time.time()
            
        except Exception as e:
            logger.error(f"Error during corruption sampling: {e}")
            # Limpiar archivo temporal en caso de error
            if sample_file and os.path.exists(sample_file):
                try:
                    os.remove(sample_file)
                except Exception:
                    pass
        finally:
            # CRÍTICO: Siempre limpiar archivo temporal para evitar acumulación
            if sample_file and os.path.exists(sample_file):
                try:
                    os.remove(sample_file)
                    logger.debug(f"Cleaned up sample file: {os.path.basename(sample_file)}")
                except Exception as e:
                    logger.warning(f"Could not remove sample file {sample_file}: {e}")
    
    @retry_on_resource_exhaustion(max_retries=3, delay=0.5)
    def _extract_video_sample(self, source_file: str, output_file: str) -> bool:
        """
        Extrae una muestra de los últimos segundos del video
        Usa managed_subprocess para garantizar cleanup de file descriptors
        
        Args:
            source_file: Archivo de video fuente
            output_file: Archivo de salida para la muestra
            
        Returns:
            bool: True si la extracción fue exitosa
        """
        try:
            # Para archivos que se están grabando en tiempo real, usar estimación basada en tiempo
            # ya que ffprobe no puede obtener duración exacta de archivos en escritura
            recording_duration = time.time() - self.recording_start_time
            
            # Estimar desde qué segundo empezar para obtener los últimos X segundos
            start_time = max(0, recording_duration - config.sample_duration_seconds)
            
            # Comando FFmpeg para extraer desde start_time por sample_duration_seconds
            cmd = [
                'ffmpeg',
                '-y',  # Sobrescribir archivo de salida
                '-v', 'quiet',  # Silencioso para evitar logs innecesarios
                '-ss', str(start_time),  # Empezar desde start_time estimado
                '-i', source_file,
                '-t', str(config.sample_duration_seconds),  # Duración exacta
                '-c', 'copy',  # Copiar sin re-encoding para velocidad
                '-avoid_negative_ts', 'make_zero',
                output_file
            ]
            
            # Usar context manager para garantizar cleanup de file descriptors
            with managed_subprocess(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            ) as process:
                try:
                    stdout, stderr = process.communicate(timeout=config.sample_analysis_timeout_seconds)
                    return_code = process.returncode
                except subprocess.TimeoutExpired:
                    logger.warning(f"Sample extraction timeout ({config.sample_analysis_timeout_seconds}s)")
                    return False
            
            # Verificar que el archivo de muestra se creó correctamente
            if return_code == 0 and os.path.exists(output_file):
                sample_size = os.path.getsize(output_file)
                if sample_size > 1024:  # > 1KB
                    return True
                else:
                    logger.warning(f"Sample too small ({sample_size} bytes), extraction may have failed")
                    return False
            else:
                logger.error(f"FFmpeg extraction failed - return code: {return_code}")
                if stderr:
                    logger.error(f"FFmpeg stderr: {stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error extracting sample: {e}")
            return False
    
    @retry_on_resource_exhaustion(max_retries=3, delay=0.5)
    def _analyze_sample_corruption(self, sample_file: str) -> Dict[str, Any]:
        """
        Analiza corrupción en la muestra usando la misma lógica del post-processing queue
        Usa managed_subprocess para garantizar cleanup de file descriptors
        
        Args:
            sample_file: Archivo de muestra a analizar
            
        Returns:
            Dict con resultado del análisis de corrupción
        """
        try:
            # Usar la misma lógica que postprocessing_queue._analyze_corruption_sync
            cmd = [
                'ffmpeg',
                '-v', 'error',
                '-i', sample_file,
                '-f', 'null', '-'
            ]
            
            stderr_output = ""
            return_code = 1
            
            # Usar context manager para garantizar cleanup de file descriptors
            with managed_subprocess(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            ) as process:
                try:
                    stdout, stderr_output = process.communicate(timeout=config.sample_analysis_timeout_seconds)
                    return_code = process.returncode
                except subprocess.TimeoutExpired:
                    logger.warning(f"Sample corruption analysis timeout")
                    return {
                        'should_discard': False,
                        'error_percentage': 0.0,
                        'critical_errors': 0,
                        'total_frames': 0,
                        'threshold': config.sample_corruption_threshold_percent
                    }
            
            errores_criticos = 0
            total_frames = 0
            
            # Patrones de errores críticos (mismo que en postprocessing_queue)
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
                total_frames = self._get_frame_count_fallback(sample_file)
            
            # Calcular porcentaje de errores
            if total_frames > 0:
                porcentaje_errores = (errores_criticos / total_frames) * 100
            else:
                porcentaje_errores = 0
            
            # Usar threshold configurado para samples
            should_discard = porcentaje_errores >= config.sample_corruption_threshold_percent
            
            return {
                'should_discard': should_discard,
                'error_percentage': porcentaje_errores,
                'critical_errors': errores_criticos,
                'total_frames': total_frames,
                'threshold': config.sample_corruption_threshold_percent
            }
            
        except Exception as e:
            logger.error(f"Error analyzing sample corruption: {e}")
            return {
                'should_discard': False,
                'error_percentage': 0.0,
                'critical_errors': 0,
                'total_frames': 0,
                'threshold': config.sample_corruption_threshold_percent
            }

    @retry_on_resource_exhaustion(max_retries=2, delay=0.3)
    def _get_frame_count_fallback(self, sample_file: str) -> int:
        """
        Obtiene el número de frames usando ffprobe como fallback
        Usa managed_subprocess para garantizar cleanup de file descriptors
        """
        try:
            probe_cmd = [
                'ffprobe', 
                '-v', 'quiet',
                '-select_streams', 'v:0',
                '-count_frames',
                '-show_entries', 'stream=nb_frames',
                '-of', 'csv=p=0',
                sample_file
            ]
            
            with managed_subprocess(
                probe_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            ) as process:
                try:
                    stdout, stderr = process.communicate(timeout=10)
                    if process.returncode == 0 and stdout.strip():
                        return int(stdout.strip())
                except subprocess.TimeoutExpired:
                    logger.warning(f"ffprobe frame count timeout")
                except ValueError:
                    logger.warning(f"Invalid frame count from ffprobe: {stdout}")
                    
        except Exception as e:
            logger.debug(f"Frame count fallback failed: {e}")
            
        # Si todo falla, retornar estimación conservadora
        return 1
    
    def get_sampling_status(self) -> Dict[str, Any]:
        """
        Obtiene el estado actual del muestreo
        """
        return {
            'is_active': self.is_sampling_active,
            'current_file': os.path.basename(self.current_file_path) if self.current_file_path else None,
            'recording_duration': time.time() - self.recording_start_time if self.recording_start_time else 0,
            'last_sample_time': self.last_sample_time,
            'configuration': {
                'enabled': config.enable_realtime_sampling,
                'interval_seconds': config.sampling_interval_seconds,
                'sample_duration_seconds': config.sample_duration_seconds,
                'corruption_threshold_percent': config.sample_corruption_threshold_percent
            }
        }
    
    def _cleanup_old_samples(self, temp_dir: str):
        """
        Limpia muestras temporales antiguas del usuario para evitar acumulación
        """
        try:
            # Buscar archivos de muestra del usuario actual
            user_samples = []
            for filename in os.listdir(temp_dir):
                if filename.startswith(f"sample_{self.user}_") and filename.endswith('.mp4'):
                    file_path = os.path.join(temp_dir, filename)
                    try:
                        mtime = os.path.getmtime(file_path)
                        user_samples.append((file_path, mtime))
                    except OSError:
                        continue
            
            # Si hay más de 3 muestras del usuario, eliminar las más antiguas
            if len(user_samples) > 3:
                # Ordenar por tiempo de modificación (más antiguas primero)
                user_samples.sort(key=lambda x: x[1])
                
                # Eliminar todas excepto las 3 más recientes
                samples_to_remove = user_samples[:-3]
                for file_path, _ in samples_to_remove:
                    try:
                        os.remove(file_path)
                        logger.debug(f"Cleaned up old sample: {os.path.basename(file_path)}")
                    except OSError as e:
                        logger.warning(f"Could not remove old sample {file_path}: {e}")
                        
        except Exception as e:
            logger.debug(f"Error during sample cleanup: {e}")