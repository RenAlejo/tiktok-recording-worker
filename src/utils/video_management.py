import os
import ffmpeg

from utils.logger_manager import logger


class VideoManagement:

    @staticmethod
    def convert_flv_to_mp4(file):
        """
        Convert the video from flv format to mp4 format with robust timestamp and gap correction
        """
        logger.info("Converting {} to MP4 format with advanced correction...".format(file))
        
        from config.env_config import config

        try:
            output_file = file.replace('_flv.mp4', '.mp4')
            
            if config.enable_advanced_ffmpeg_correction:
                # Estrategia avanzada: detectar y corregir problemas específicos
                logger.info("Using advanced FFmpeg correction strategy")
                
                # Primer intento: corrección robusta con filtros
                try:
                    ffmpeg.input(file).output(
                        output_file,
                        vcodec='libx264',  # Re-encode para corregir problemas
                        acodec='aac',      # Re-encode audio también
                        **{
                            'avoid_negative_ts': 'make_zero',
                            'fflags': '+genpts+igndts',  # Ignorar timestamps corruptos y regenerar
                            'vsync': 'cfr',             # Forzar framerate constante
                            'r': '30',                  # Forzar 30 FPS para consistency
                            'max_muxing_queue_size': '9999',
                            'crf': '23',                # Calidad razonable para re-encode
                            'preset': 'faster',         # Velocidad de encoding
                            'y': '-y'
                        }
                    ).run(quiet=True)
                    
                    logger.info("Advanced correction successful")
                    
                except ffmpeg.Error as e:
                    logger.warning(f"Advanced correction failed, trying standard method: {e}")
                    # Fallback a método estándar
                    ffmpeg.input(file).output(
                        output_file,
                        c='copy',
                        avoid_negative_ts='make_zero',
                        fflags='+genpts',
                        y='-y'
                    ).run(quiet=True)
            else:
                # Método estándar con metadatos optimizados para videos verticales TikTok
                try:
                    # Primero intentar con stream copy manteniendo metadatos
                    ffmpeg.input(file).output(
                        output_file,
                        c='copy',
                        avoid_negative_ts='make_zero',
                        fflags='+genpts',
                        metadata='rotate=0',  # Forzar rotación 0
                        **{
                            'movflags': '+faststart',  # Optimizar para streaming
                            'y': '-y'
                        }
                    ).run(quiet=True)
                except ffmpeg.Error as copy_error:
                    logger.warning(f"Stream copy failed, trying with re-encoding: {copy_error}")
                    # Fallback: re-encode con configuraciones específicas para TikTok
                    """ffmpeg.input(file).output(
                        output_file,
                        vcodec='libx264',
                        acodec='aac',
                        avoid_negative_ts='make_zero',
                        fflags='+genpts',
                        metadata='rotate=0',
                        **{
                            'movflags': '+faststart',
                            'preset': 'faster',
                            'crf': '23',
                            'y': '-y'
                        }
                    ).run(quiet=True)"""
            
        except ffmpeg.Error as e:
            logger.error(f"ffmpeg error during conversion: {e.stderr.decode() if hasattr(e, 'stderr') else str(e)}")
            # Fallback final
            try:
                logger.info("Attempting basic conversion as final fallback...")
                ffmpeg.input(file).output(
                    output_file,
                    c='copy',
                    y='-y'
                ).run(quiet=True)
            except ffmpeg.Error as e2:
                logger.error(f"All conversion methods failed: {e2.stderr.decode() if hasattr(e2, 'stderr') else str(e2)}")
                return

        # Solo eliminar archivo original si la conversión fue exitosa
        try:
            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                os.remove(file)
                logger.info("Finished converting {} with advanced corrections\n".format(file))
            else:
                logger.error("Converted file is empty or doesn't exist. Keeping original.")
        except Exception as e:
            logger.error(f"Error checking converted file: {e}")

    @staticmethod
    def fix_video_timestamps(input_file, output_file):
        """
        Fix video timestamps and remove gaps for severely corrupted files
        """
        logger.info(f"Applying advanced timestamp fix to {input_file}...")
        
        try:
            # Método más agresivo para archivos con gaps severos
            ffmpeg.input(input_file).output(
                output_file,
                c='copy',
                avoid_negative_ts='make_zero',
                fflags='+genpts+igndts',  # Ignorar timestamps originales y regenerar
                map_metadata=-1,  # Remover metadata que puede contener timestamps corruptos
                y='-y'
            ).run(quiet=True)
            
            logger.info(f"Advanced timestamp fix completed: {output_file}")
            return True
            
        except ffmpeg.Error as e:
            logger.error(f"Advanced timestamp fix failed: {e.stderr.decode() if hasattr(e, 'stderr') else str(e)}")
            return False

    @staticmethod
    def _check_video_duration(video_file):
        """
        Check if video duration is suspicious (indicates timestamp issues)
        """
        try:
            # Obtener información del video usando ffprobe
            probe = ffmpeg.probe(video_file)
            duration = float(probe['streams'][0]['duration'])
            file_size = os.path.getsize(video_file)
            
            # Calcular bitrate aproximado (bytes por segundo)
            bitrate = file_size / duration if duration > 0 else 0
            
            # Detectar duraciones sospechosas
            suspicious = False
            reason = ""
            
            # Duración extremadamente larga (> 6 horas) es sospechosa
            if duration > 21600:  # 6 horas
                suspicious = True
                reason = "Duration exceeds 6 hours"
            
            # Bitrate extremadamente bajo indica gaps
            elif bitrate < 10000:  # < 10KB/s es muy bajo para video
                suspicious = True
                reason = f"Very low bitrate: {bitrate:.0f} bytes/s"
            
            # Ratio duración/tamaño sospechoso
            elif duration > 3600 and file_size < 50 * 1024 * 1024:  # > 1h pero < 50MB
                suspicious = True
                reason = f"Duration {duration/3600:.1f}h but only {file_size/(1024*1024):.1f}MB"
            
            return {
                'duration': duration,
                'file_size': file_size,
                'bitrate': bitrate,
                'suspicious': suspicious,
                'reason': reason
            }
            
        except Exception as e:
            logger.warning(f"Could not check video duration: {e}")
            return {
                'duration': 0,
                'file_size': 0,
                'bitrate': 0,
                'suspicious': False,
                'reason': 'Could not analyze'
            }
