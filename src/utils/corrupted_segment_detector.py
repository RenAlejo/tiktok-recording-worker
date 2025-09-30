"""
Detector optimizado de segmentos corruptos/audio-only para integración en post-procesamiento.
Detecta específicamente segmentos que pueden causar problemas en la concatenación.
"""

import os
import subprocess
import json
from typing import Dict, Any, List
from utils.logger_manager import logger
from config.env_config import config


class CorruptedSegmentDetector:
    """Detector eficiente de segmentos problemáticos para post-procesamiento"""
    
    @staticmethod
    def is_corrupted_segment(file_path: str, log_prefix: str = "") -> Dict[str, Any]:
        """
        Detecta si un segmento está corrupto o es problemático para concatenación.
        Optimizado para rendimiento en producción.
        
        Args:
            file_path: Ruta al archivo
            log_prefix: Prefijo para logs
            
        Returns:
            Dict con resultado de detección:
            {
                'is_corrupted': bool,
                'reason': str,
                'file_size_kb': float,
                'should_exclude': bool
            }
        """
        result = {
            'is_corrupted': False,
            'reason': '',
            'file_size_kb': 0.0,
            'should_exclude': False
        }
        
        try:
            # Verificar existencia y tamaño
            if not os.path.exists(file_path):
                result.update({
                    'is_corrupted': True,
                    'reason': 'File not found',
                    'should_exclude': True
                })
                return result
            
            file_size_bytes = os.path.getsize(file_path)
            result['file_size_kb'] = round(file_size_bytes / 1024, 2)
            
            # Archivos extremadamente pequeños (< 1KB) son definitivamente corruptos
            if file_size_bytes < 1024:
                result.update({
                    'is_corrupted': True,
                    'reason': f'Extremely small file ({result["file_size_kb"]} KB)',
                    'should_exclude': True
                })
                logger.warning(f"{log_prefix} Corrupted segment: {os.path.basename(file_path)} - {result['reason']}")
                return result
            
            # Solo hacer análisis detallado en archivos sospechosos (< configurado MB)
            # Esto optimiza el rendimiento al evitar análisis innecesarios en archivos grandes
            max_size_mb = config.corrupted_segment_max_file_size_mb
            if result['file_size_kb'] > (max_size_mb * 1024):
                # Archivo grande, asumir válido (optimización de rendimiento)
                return result
            
            # Análisis detallado solo para archivos pequeños sospechosos
            corruption_detected = CorruptedSegmentDetector._analyze_small_segment(file_path, result['file_size_kb'], log_prefix)
            
            if corruption_detected:
                result.update(corruption_detected)
            
        except Exception as e:
            logger.error(f"{log_prefix} Error analyzing segment {os.path.basename(file_path)}: {e}")
            result.update({
                'is_corrupted': True,
                'reason': f'Analysis error: {e}',
                'should_exclude': True
            })
        
        return result
    
    @staticmethod
    def _analyze_small_segment(file_path: str, file_size_kb: float, log_prefix: str) -> Dict[str, Any]:
        """
        Análisis detallado para archivos pequeños sospechosos.
        """
        try:
            # Usar ffprobe para análisis rápido
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_streams',
                '-select_streams', 'v:0',  # Solo analizar primer stream de video
                file_path
            ]
            
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=config.corrupted_segment_ffprobe_timeout_seconds
            )
            
            if process.returncode != 0:
                return {
                    'is_corrupted': True,
                    'reason': 'Cannot analyze video streams',
                    'should_exclude': True
                }
            
            probe_data = json.loads(process.stdout)
            streams = probe_data.get('streams', [])
            
            if not streams:
                return {
                    'is_corrupted': True,
                    'reason': 'No video streams found',
                    'should_exclude': True
                }
            
            video_stream = streams[0]
            
            # Aplicar criterios específicos de detección
            corruption_reason = CorruptedSegmentDetector._check_video_stream_integrity(video_stream, file_size_kb)
            
            if corruption_reason:
                logger.warning(f"{log_prefix} Corrupted segment: {os.path.basename(file_path)} - {corruption_reason}")
                return {
                    'is_corrupted': True,
                    'reason': corruption_reason,
                    'should_exclude': True
                }
            
        except subprocess.TimeoutExpired:
            logger.warning(f"{log_prefix} ffprobe timeout for {os.path.basename(file_path)}")
            return {
                'is_corrupted': True,
                'reason': 'Analysis timeout',
                'should_exclude': True
            }
        except json.JSONDecodeError:
            return {
                'is_corrupted': True,
                'reason': 'Invalid ffprobe output',
                'should_exclude': True
            }
        except Exception as e:
            logger.error(f"{log_prefix} Unexpected error analyzing {os.path.basename(file_path)}: {e}")
            return {
                'is_corrupted': True,
                'reason': f'Analysis failed: {e}',
                'should_exclude': True
            }
        
        return {}  # No corruption detected
    
    @staticmethod
    def _check_video_stream_integrity(video_stream: Dict, file_size_kb: float) -> str:
        """
        Verifica la integridad del stream de video usando criterios específicos.
        Basado en el análisis del archivo problemático identificado.
        """
        # Criterio 1: Level anómalo (level: -99 indica corrupción)
        level = video_stream.get('level', 0)
        if level < 0:
            return f"Invalid video level ({level})"
        
        # Criterio 2: Duración contradictoria en tags
        tags = video_stream.get('tags', {})
        tag_duration = tags.get('DURATION', '')
        
        if tag_duration and '00:00:00.000000000' in tag_duration:
            # El tag indica duración 0 pero ffprobe reporta duración > 0
            stream_duration = video_stream.get('duration', '0')
            if stream_duration and float(stream_duration) > 0:
                return f"Contradictory duration (stream: {stream_duration}s, tag: 0s)"
        
        # Criterio 3: Archivo muy pequeño con duración reportada (sospechoso)
        if file_size_kb < 10:  # < 10KB
            duration = video_stream.get('duration', '0')
            if duration and float(duration) > 0.1:  # Duración > 0.1s pero archivo muy pequeño
                return f"Suspiciously small file ({file_size_kb} KB) with reported duration ({duration}s)"
        
        # Criterio 4: Verificar nb_frames si está disponible
        nb_frames = video_stream.get('nb_frames')
        if nb_frames is not None:
            try:
                frame_count = int(nb_frames)
                if frame_count == 0:
                    return "Zero frames in video stream"
            except (ValueError, TypeError):
                pass
        
        return ""  # No corruption indicators found
    
    @staticmethod
    def filter_valid_segments(file_paths: List[str], log_prefix: str = "") -> Dict[str, Any]:
        """
        Filtra una lista de segmentos excluyendo los corruptos.
        Optimizado para procesamiento en lote.
        """
        valid_segments = []
        corrupted_segments = []
        filter_stats = {
            'total_analyzed': 0,
            'corrupted_found': 0,
            'total_size_filtered_kb': 0
        }
        
        logger.debug(f"{log_prefix} Checking {len(file_paths)} segments for corruption")
        
        for file_path in file_paths:
            filter_stats['total_analyzed'] += 1
            
            detection_result = CorruptedSegmentDetector.is_corrupted_segment(file_path, log_prefix)
            
            if detection_result['should_exclude']:
                corrupted_segments.append({
                    'file_path': file_path,
                    'reason': detection_result['reason'],
                    'file_size_kb': detection_result['file_size_kb']
                })
                filter_stats['corrupted_found'] += 1
                filter_stats['total_size_filtered_kb'] += detection_result['file_size_kb']
                
                # Eliminar archivo corrupto inmediatamente
                try:
                    os.remove(file_path)
                    logger.info(f"{log_prefix} Deleted corrupted segment: {os.path.basename(file_path)} - {detection_result['reason']}")
                except Exception as e:
                    logger.error(f"{log_prefix} Failed to delete {os.path.basename(file_path)}: {e}")
            else:
                valid_segments.append(file_path)
        
        # Log resumen solo si hay filtros
        if filter_stats['corrupted_found'] > 0:
            logger.info(f"{log_prefix} Filtered {filter_stats['corrupted_found']} corrupted segments")
        
        return {
            'valid_segments': valid_segments,
            'corrupted_segments': corrupted_segments,
            'filter_stats': filter_stats
        }


# Función de conveniencia para uso rápido
def is_audio_only_or_corrupted(file_path: str) -> bool:
    """
    Función simple para verificar si un segmento debe excluirse de concatenación
    
    Args:
        file_path: Ruta al archivo
        
    Returns:
        bool: True si debe excluirse, False si es válido
    """
    result = CorruptedSegmentDetector.is_corrupted_segment(file_path)
    return result['should_exclude']