import asyncio
import time

from pathlib import Path

from pyrogram import Client
from pyrogram.enums import ParseMode

from utils.logger_manager import logger
from utils.utils import read_telegram_config
from config.env_config import config


FREE_USER_MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
PREMIUM_USER_MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024


class Telegram:
    def __init__(self, app: Client):
        """
        Recibe la instancia de Client existente en lugar de crear una nueva
        """
        self.app = app
        config = read_telegram_config()
        self.chat_id = config["chat_id"]
        
    def diagnose_video_file(self, file_path):
        """
        Método de diagnóstico mejorado para verificar detalles del archivo de video
        Usa FFprobe para obtener metadatos más precisos
        """
        try: 
            import ffmpeg
            
            # Obtener información detallada con FFprobe
            probe = ffmpeg.probe(file_path)
            video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
            
            if not video_stream:
                logger.error(f"No video stream found in {file_path}")
                return None
            
            width = int(video_stream['width'])
            height = int(video_stream['height'])
            duration = float(video_stream.get('duration', 0))
            
            # Verificar metadatos de rotación
            rotation = 0
            if 'tags' in video_stream:
                # Buscar metadatos de rotación
                rotation_tag = video_stream['tags'].get('rotate', '0')
                try:
                    rotation = int(rotation_tag)
                except:
                    rotation = 0
            
            # Verificar aspect ratio
            display_aspect_ratio = video_stream.get('display_aspect_ratio', 'N/A')
            sample_aspect_ratio = video_stream.get('sample_aspect_ratio', 'N/A')
            
            # Log detallado para debug
            #logger.info(f"Video metadata for {Path(file_path).name}:")
            #logger.info(f"  Resolution: {width}x{height}")
            #logger.info(f"  Duration: {duration:.2f}s")
            #logger.info(f"  Rotation: {rotation}°")
            #logger.info(f"  Display AR: {display_aspect_ratio}")
            #logger.info(f"  Sample AR: {sample_aspect_ratio}")
            
            return {
                'width': width,
                'height': height,
                'duration': duration,
                'rotation': rotation,
                'display_aspect_ratio': display_aspect_ratio,
                'sample_aspect_ratio': sample_aspect_ratio,
                'codec': video_stream.get('codec_name', 'unknown')
            }
        
        except Exception as e:
            logger.error(f"Video diagnostics error: {e}")
            # Fallback a método básico
            try:
                import cv2
                video = cv2.VideoCapture(file_path)
                width = int(video.get(cv2.CAP_PROP_FRAME_WIDTH))
                height = int(video.get(cv2.CAP_PROP_FRAME_HEIGHT))
                fps = video.get(cv2.CAP_PROP_FPS)
                total_frames = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
                duration = total_frames / fps if fps > 0 else 0
                video.release()
                
                return {
                    'width': width,
                    'height': height,
                    'duration': duration,
                    'rotation': 0,
                    'display_aspect_ratio': 'N/A',
                    'sample_aspect_ratio': 'N/A',
                    'codec': 'unknown'
                }
            except Exception as fallback_error:
                logger.error(f"Fallback video diagnostics error: {fallback_error}")
                return None

    def _format_duration(self, seconds: float) -> str:
        """
        Formatea duración en segundos a formato HH:MM:SS

        Args:
            seconds: Duración en segundos

        Returns:
            String con formato "HH:MM:SS"
        """
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    def _format_file_size(self, bytes_size: int) -> str:
        """
        Formatea tamaño de archivo en bytes a formato legible (GB/MB)

        Args:
            bytes_size: Tamaño en bytes

        Returns:
            String con formato "X.XX GB" o "X.XX MB"
        """
        gb = bytes_size / (1024 ** 3)
        if gb >= 1.0:
            return f"{gb:.2f} GB"
        else:
            mb = bytes_size / (1024 ** 2)
            return f"{mb:.2f} MB"

    def _get_recording_date(self, file_path: str) -> str:
        """
        Obtiene la fecha de modificación del archivo en formato YYYY-MM-DD

        Args:
            file_path: Ruta del archivo

        Returns:
            String con formato "YYYY-MM-DD"
        """
        from datetime import datetime
        file_stat = Path(file_path).stat()
        modification_time = datetime.fromtimestamp(file_stat.st_mtime)
        return modification_time.strftime("%Y-%m-%d")

    def _build_caption(
        self,
        username: str,
        file_path: str,
        file_size: int,
        video_info: dict,
        fragment_number: int,
        is_fragmented: bool
    ) -> str:
        """
        Construye el caption del video de forma dinámica basado en la configuración

        Args:
            username: Nombre de usuario de TikTok
            file_path: Ruta del archivo de video
            file_size: Tamaño del archivo en bytes
            video_info: Diccionario con información del video (incluye 'duration')
            fragment_number: Número del fragmento actual
            is_fragmented: Si el video está fragmentado

        Returns:
            Caption formateado según la configuración
        """
        # Primera línea siempre fija: username
        caption_lines = [f'🔗#{username}']

        # Línea de fragmento (opcional y condicional)
        if config.caption_show_fragment_info and is_fragmented:
            caption_lines.append(f'🎬Part {fragment_number}')

        # Línea de fecha (opcional)
        if config.caption_show_date:
            recording_date = self._get_recording_date(file_path)
            caption_lines.append(f'📅 {recording_date}')

        # Línea de info del video (opcional)
        if config.caption_show_video_info:
            duration = self._format_duration(video_info['duration'])
            size = self._format_file_size(file_size)
            caption_lines.append(f'⏱️ {duration} | 💾 {size}')

        # Línea de watermark (opcional)
        if config.caption_show_watermark:
            caption_lines.append(config.caption_watermark_text)

        # Unir todas las líneas con saltos de línea y agregar salto final
        return '\n'.join(caption_lines) + '\n'

    async def upload(self, file_path: str, chat_id: int, username: str, fragment_number: int = 1, is_fragmented: bool = False, max_retries=3, collage_path: str = None, thumbnail_path: str = None):
        def progress_callback(current, total):
            elapsed = time.time() - self._upload_start_time if hasattr(self, '_upload_start_time') else 0.001
            speed = current / elapsed  # bytes/segundo
            percent = (current / total) * 100 if total else 0
            speed_mb = speed / (1024 * 1024)
            logger.info(f"Subiendo: {percent:.2f}% - Velocidad: {speed_mb:.2f} MB/s ({current}/{total} bytes)")

        for attempt in range(max_retries):
            try:
                # El cliente ya debe estar conectado por el bot principal
                if not self.app.is_connected:
                    logger.warning("Telegram client is not connected for upload, but avoiding start() to prevent reinitialization")

                # Inicializar tiempo de subida para el callback de progreso
                if attempt == 0:
                    self._upload_start_time = time.time()

                video_info = self.diagnose_video_file(file_path)
                if not video_info:
                    logger.error(f"Could not get video information for {file_path}")
                    return False

                me = await self.app.get_me()
                is_premium = me.is_premium
                max_size = (
                    PREMIUM_USER_MAX_FILE_SIZE 
                    if is_premium else FREE_USER_MAX_FILE_SIZE
                )

                file_size = Path(file_path).stat().st_size
                if file_size > max_size:
                    logger.warning("File too large for upload")
                    return False

                # Construir caption dinámico basado en configuración
                caption = self._build_caption(
                    username=username,
                    file_path=file_path,
                    file_size=file_size,
                    video_info=video_info,
                    fragment_number=fragment_number,
                    is_fragmented=is_fragmented
                )

                # Enviar video y collage en el mismo mensaje si el collage existe
                if collage_path and Path(collage_path).exists():
                    try:
                        # Enviar como grupo de medios (video + foto) con una sola caption
                        from pyrogram.types import InputMediaPhoto, InputMediaVideo
                        
                        # Para videos verticales, optimizar parámetros para Telegram Desktop
                        video_width = video_info['width']
                        video_height = video_info['height']
                        
                        # Detectar si es vertical y ajustar configuración
                        is_vertical = video_height > video_width
                        
                        #logger.info(f"Video dimensions: {video_width}x{video_height} (vertical: {is_vertical})")
                        
                        media_group = [
                            InputMediaVideo(
                                media=file_path,
                                caption=caption,
                                supports_streaming=True,
                                width=video_width,
                                height=video_height, 
                                duration=int(video_info['duration']),
                                thumb=thumbnail_path if is_vertical and thumbnail_path else None  # Usar thumbnail random para videos verticales
                            ),
                            InputMediaPhoto(media=collage_path)
                        ]
                        
                        sent_messages = await self.app.send_media_group(
                            chat_id=chat_id,
                            media=media_group
                        )
                        
                        logger.info(f"Video and collage sent successfully for {username}")
                        
                        # Retornar todos los message_ids del media group para reenvío completo
                        if sent_messages and len(sent_messages) > 0:
                            message_ids = [msg.id for msg in sent_messages]
                            return message_ids  # Retornar lista de message_ids para media group
                        
                        # NOTE: Collage and thumbnail cleanup is now handled by UploadManager
                        # to avoid race conditions between main upload and backup upload
                            
                    except Exception as media_group_error:
                        logger.warning(f"Failed to send media group for {username}: {media_group_error}")
                        # Fallback: enviar video solo
                        sent_message = await self.app.send_video(
                            chat_id=chat_id,
                            video=file_path,
                            caption=caption,
                            parse_mode=ParseMode.HTML,
                            supports_streaming=True,
                            width=video_info['width'],
                            height=video_info['height'],
                            duration=int(video_info['duration'])
                        )
                        
                        if sent_message:
                            return sent_message.id  # Retornar message_id
                        
                else:
                    # Enviar solo el video si no hay collage
                    video_width = video_info['width']
                    video_height = video_info['height']
                    is_vertical = video_height > video_width
                    
                    #logger.info(f"Video dimensions: {video_width}x{video_height} (vertical: {is_vertical})")
                    
                    sent_message = await self.app.send_video(
                        chat_id=chat_id,
                        video=file_path,
                        caption=caption,
                        parse_mode=ParseMode.HTML,
                        supports_streaming=True,
                        width=video_width,
                        height=video_height,
                        duration=int(video_info['duration']),
                        #progress=progress_callback
                    )

                logger.info(f"Successfully uploaded video to chat {chat_id}")

                # NOTE: File deletion is now handled by the UploadManager to avoid race conditions
                # between main upload and backup upload. Do not delete files here.

                await asyncio.sleep(0)
                
                # Retornar message_id en lugar de True para permitir forwarding
                if sent_message:
                    return sent_message.id
                else:
                    return True  # Fallback para compatibilidad

            except Exception as e:
                logger.warning(
                    f"Upload attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {2 ** attempt} seconds..."
                )
                await asyncio.sleep(2 ** attempt)

        logger.error("Max retries reached. Upload failed.")
        return False

    async def forward_video_to_backup(self, message_ids, source_chat_id: int, backup_chat_id: int, username: str):
        """
        Reenvía mensaje(s) de video al chat de backup usando message forwarding
        Soporta tanto mensajes individuales como media groups (video + collage)
        Esto ahorra ancho de banda al no subir el archivo nuevamente
        """
        # Normalizar message_ids a lista para manejo uniforme
        if isinstance(message_ids, int):
            message_ids = [message_ids]
        elif not isinstance(message_ids, list):
            logger.error(f"Invalid message_ids type for {username}: {type(message_ids)}")
            return False
            
        for attempt in range(config.forwarding_retry_attempts):
            try:
                # El cliente ya debe estar conectado por el bot principal
                if not self.app.is_connected:
                    logger.warning("Telegram client is not connected for forwarding, but avoiding start() to prevent reinitialization")

                # Reenviar todos los mensajes (para media groups completos)
                forwarded_messages = await self.app.forward_messages(
                    chat_id=backup_chat_id,
                    from_chat_id=source_chat_id,
                    message_ids=message_ids
                )
                
                if forwarded_messages:
                    message_count = len(message_ids)
                    if message_count > 1:
                        logger.info(f"Successfully forwarded media group ({message_count} messages) for {username} to backup chat")
                    else:
                        logger.info(f"Successfully forwarded video message for {username} to backup chat")
                    return True
                else:
                    logger.warning(f"Forward returned None for {username}")
                    return False

            except Exception as e:
                logger.warning(
                    f"Forward attempt {attempt + 1} failed for {username}: {e}. "
                    f"Retrying in {config.forwarding_retry_delay_seconds} seconds..."
                )
                if attempt < config.forwarding_retry_attempts - 1:
                    await asyncio.sleep(config.forwarding_retry_delay_seconds)

        logger.error(f"Max forwarding retries reached for {username}. Forward failed.")
        return False

    async def close(self):
        """
        Método para cerrar la conexión si es necesario
        """
        if self.app.is_connected:
            await self.app.stop()