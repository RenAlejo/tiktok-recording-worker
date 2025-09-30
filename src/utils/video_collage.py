"""
Módulo para generar collages de screenshots de videos
Extrae capturas distribuidas uniformemente y crea un grid con timestamps
"""
import os
import math
import tempfile
import ffmpeg
from PIL import Image, ImageDraw, ImageFont
from typing import List, Tuple, Optional
from utils.logger_manager import logger
from config.env_config import config


class VideoCollageGenerator:
    """Generador de collages de video con screenshots y timestamps"""
    
    def __init__(self):
        self.temp_dir = None
    
    def generate_collage(self, video_path: str, output_path: str) -> bool:
        """
        Genera un collage de screenshots del video
        
        Args:
            video_path: Ruta al archivo de video
            output_path: Ruta donde guardar el collage
            
        Returns:
            bool: True si se generó exitosamente, False en caso de error
        """
        if not config.enable_video_collage:
            logger.debug("Video collage generation is disabled")
            return False
            
        if not os.path.exists(video_path):
            logger.error(f"Video file not found: {video_path}")
            return False
        
        try:
            # Crear directorio temporal para screenshots
            self.temp_dir = tempfile.mkdtemp(prefix="collage_")
            logger.info(f"Generating video collage for: {os.path.basename(video_path)}")
            
            # Obtener duración del video
            duration = self._get_video_duration(video_path)
            if duration <= 0:
                logger.error("Could not determine video duration")
                return False
            
            # Extraer screenshots
            screenshot_paths, timestamps = self._extract_screenshots(video_path, duration)
            if not screenshot_paths:
                logger.error("Failed to extract screenshots")
                return False
            
            # Crear collage
            success = self._create_collage(screenshot_paths, timestamps, output_path)
            
            if success:
                logger.info(f"Video collage generated successfully: {output_path}")
            else:
                logger.error("Failed to create collage")
            
            return success
            
        except Exception as e:
            logger.error(f"Error generating video collage: {e}")
            return False
        finally:
            # Limpiar archivos temporales
            self._cleanup_temp_files()
    
    def _get_video_duration(self, video_path: str) -> float:
        """Obtiene la duración del video en segundos"""
        try:
            probe = ffmpeg.probe(video_path)
            duration = float(probe['streams'][0]['duration'])
            logger.debug(f"Video duration: {duration:.1f} seconds")
            return duration
        except Exception as e:
            logger.error(f"Error getting video duration: {e}")
            return 0.0
    
    def _extract_screenshots(self, video_path: str, duration: float) -> Tuple[List[str], List[str]]:
        """
        Extrae screenshots distribuidos uniformemente en el video
        
        Returns:
            Tuple[List[str], List[str]]: (paths_screenshots, timestamps)
        """
        screenshot_count = min(config.collage_screenshot_count, 16)  # Máximo 16 para evitar grid muy denso
        screenshot_paths = []
        timestamps = []
        
        try:
            # Calcular intervalos de tiempo
            if duration < screenshot_count:
                # Si el video es muy corto, tomar screenshots cada segundo
                time_intervals = list(range(int(duration)))
            else:
                # Distribuir uniformemente
                time_intervals = [i * duration / screenshot_count for i in range(screenshot_count)]
            
            for i, timestamp in enumerate(time_intervals):
                # Evitar el primer y último segundo para tener mejor contenido
                safe_timestamp = max(1, min(timestamp, duration - 1))
                
                screenshot_path = os.path.join(self.temp_dir, f"screenshot_{i:02d}.jpg")
                
                # Extraer screenshot usando FFmpeg
                try:
                    ffmpeg.input(video_path, ss=safe_timestamp).output(
                        screenshot_path,
                        vframes=1,
                        format='image2',
                        vcodec='mjpeg',
                        s='180x320'  # Formato vertical optimizado para collage (menos ancho)
                    ).run(quiet=True, overwrite_output=True)
                    
                    if os.path.exists(screenshot_path) and os.path.getsize(screenshot_path) > 0:
                        screenshot_paths.append(screenshot_path)
                        # Formatear timestamp como HH:MM:SS o MM:SS según la duración
                        hours = int(safe_timestamp // 3600)
                        minutes = int((safe_timestamp % 3600) // 60)
                        seconds = int(safe_timestamp % 60)
                        if hours > 0:
                            timestamps.append(f"{hours}:{minutes:02d}:{seconds:02d}")
                        else:
                            timestamps.append(f"{minutes:02d}:{seconds:02d}")
                        logger.debug(f"Extracted screenshot {i+1}/{screenshot_count} at {timestamps[-1]}")
                    
                except ffmpeg.Error as e:
                    logger.warning(f"Failed to extract screenshot at {safe_timestamp}s: {e}")
                    continue
            
            logger.info(f"Successfully extracted {len(screenshot_paths)} screenshots")
            return screenshot_paths, timestamps
            
        except Exception as e:
            logger.error(f"Error extracting screenshots: {e}")
            return [], []
    
    def _create_collage(self, screenshot_paths: List[str], timestamps: List[str], output_path: str) -> bool:
        """Crea el collage con las screenshots y timestamps"""
        try:
            if not screenshot_paths:
                return False
            
            # Configuración del grid
            rows = config.collage_grid_rows
            cols = config.collage_grid_cols
            total_slots = rows * cols
            
            # Ajustar número de screenshots al grid disponible
            screenshots_to_use = screenshot_paths[:total_slots]
            timestamps_to_use = timestamps[:total_slots]
            
            # Dimensiones del collage
            collage_width = config.collage_width
            collage_height = config.collage_height
            
            # Reservar espacio para el header con watermark
            header_height = 60
            grid_height = collage_height - header_height
            
            # Calcular dimensiones de cada celda
            cell_width = collage_width // cols
            cell_height = grid_height // rows
            
            # Crear imagen del collage con fondo negro
            collage = Image.new('RGB', (collage_width, collage_height), 'grey')
            draw = ImageDraw.Draw(collage)
            
            # Intentar cargar fuente, usar fuente por defecto si falla
            try:
                # Fuente para watermark
                watermark_font = ImageFont.truetype("arial.ttf", 24)
                timestamp_font = ImageFont.truetype("arial.ttf", 15)
            except:
                # Fuente por defecto si no encuentra arial
                watermark_font = ImageFont.load_default()
                timestamp_font = ImageFont.load_default()
            
            # Dibujar watermark en la parte superior
            watermark_text = config.collage_watermark_text
            # Calcular posición centrada para el watermark
            bbox = draw.textbbox((0, 0), watermark_text, font=watermark_font)
            text_width = bbox[2] - bbox[0]
            watermark_x = (collage_width - text_width) // 2
            watermark_y = (header_height - 24) // 2
            
            draw.text((watermark_x, watermark_y), watermark_text, fill='white', font=watermark_font)
            
            # Colocar screenshots en el grid
            for i, (screenshot_path, timestamp) in enumerate(zip(screenshots_to_use, timestamps_to_use)):
                if i >= total_slots:
                    break
                
                row = i // cols
                col = i % cols
                
                # Calcular posición
                x = col * cell_width
                y = header_height + row * cell_height
                
                try:
                    # Cargar y redimensionar screenshot
                    screenshot = Image.open(screenshot_path)
                    
                    # Redimensionar manteniendo aspecto, con gaps más pequeños
                    img_width = cell_width - 1   # Margen reducido de 3px a cada lado
                    img_height = cell_height - 25  # Espacio reducido para timestamp
                    
                    screenshot.thumbnail((img_width, img_height), Image.Resampling.LANCZOS)
                    
                    # Centrar imagen en la celda
                    paste_x = x + (cell_width - screenshot.width) // 2
                    paste_y = y + 3  # Margen superior reducido
                    
                    collage.paste(screenshot, (paste_x, paste_y))
                    
                    # Agregar timestamp debajo de la imagen
                    timestamp_bbox = draw.textbbox((0, 0), timestamp, font=timestamp_font)
                    timestamp_width = timestamp_bbox[2] - timestamp_bbox[0]
                    timestamp_x = x + (cell_width - timestamp_width) // 2
                    timestamp_y = paste_y + screenshot.height + 3  # Gap reducido para timestamp
                    
                    draw.text((timestamp_x, timestamp_y), timestamp, fill='white', font=timestamp_font)
                    
                except Exception as e:
                    logger.warning(f"Error processing screenshot {i}: {e}")
                    continue
            
            # Guardar collage
            collage.save(output_path, 'JPEG', quality=90)
            #logger.info(f"Collage saved with {len(screenshots_to_use)} screenshots")
            return True
            
        except Exception as e:
            logger.error(f"Error creating collage: {e}")
            return False
    
    def _cleanup_temp_files(self):
        """Limpia archivos temporales"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                import shutil
                shutil.rmtree(self.temp_dir)
                logger.debug(f"Cleaned up temporary directory: {self.temp_dir}")
            except Exception as e:
                logger.warning(f"Could not clean up temp directory: {e}")
            finally:
                self.temp_dir = None