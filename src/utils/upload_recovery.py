"""
Sistema de recuperación de uploads para videos que no se pudieron procesar
debido a errores de sistema como "Too many open files"
"""

import os
import json
import time
import glob
import re
from typing import List, Dict, Any, Optional
from pathlib import Path

from utils.logger_manager import logger
from config.env_config import config


class UploadRecoveryScanner:
    """
    Escanea el directorio de grabaciones en busca de videos MP4 que no fueron
    enviados a la cola de upload debido a errores del sistema
    """
    
    def __init__(self):
        self.base_directory = config.output_directory
        self.state_file = os.path.join(self.base_directory, "recovery_state.json")
        self.recovered_files = self._load_recovery_state()
        self.recovery_timestamps = self._load_recovery_timestamps()
        
    def scan_for_pending_uploads(self) -> List[Dict[str, Any]]:
        """
        Escanea recursivamente el directorio de grabaciones buscando videos MP4
        que parecen estar completos pero no fueron procesados
        
        Returns:
            Lista de archivos pendientes de upload con metadata
        """
        if not config.upload_recovery_scan_enabled:
            logger.debug("Upload recovery scan disabled by configuration")
            return []
            
        logger.info(f"🔍 Starting upload recovery scan in: {self.base_directory}")
        pending_files = []
        
        try:
            # Buscar archivos MP4 en todas las subcarpetas
            pattern = os.path.join(self.base_directory, "**", "*.mp4")
            mp4_files = glob.glob(pattern, recursive=True)
            
            logger.info(f"🔍 Found {len(mp4_files)} MP4 files total")
            
            for mp4_file in mp4_files:
                if self._should_recover_file(mp4_file):
                    file_info = self._extract_file_metadata(mp4_file)
                    if file_info:
                        pending_files.append(file_info)
                        logger.info(f"📤 Recovery candidate: {file_info['username']} - {os.path.basename(mp4_file)}")
                    else:
                        logger.debug(f"⚠️ Could not extract metadata: {os.path.basename(mp4_file)}")
                else:
                    logger.debug(f"🚫 Skipped by recovery rules: {os.path.basename(mp4_file)}")
                        
            logger.info(f"📥 Found {len(pending_files)} files pending upload recovery")
            return pending_files
            
        except Exception as e:
            logger.error(f"❌ Error during upload recovery scan: {e}")
            return []
    
    def _should_recover_file(self, file_path: str) -> bool:
        """
        Determina si un archivo MP4 debe ser recuperado para upload
        Solo procesa videos completos finales, nunca partes fragmentadas
        """
        try:
            filename = os.path.basename(file_path)
            
            # Skip si ya fue procesado recientemente (con TTL)
            if file_path in self.recovered_files:
                recovery_time = self._get_recovery_timestamp(file_path)
                if recovery_time and (time.time() - recovery_time) < 3600:  # 1 hora TTL
                    return False
                # Si pasó 1 hora desde el último intento, permitir retry
                
            # Skip archivos muy recientes (pueden estar siendo procesados)
            file_age = time.time() - os.path.getmtime(file_path)
            if file_age < 120:  # Menos de 2 minutos (más permisivo)
                return False
                
            # Skip archivos muy pequeños (probablemente corruptos)
            file_size = os.path.getsize(file_path)
            if file_size < 1024 * 1024:  # < 1MB
                return False
            
            # CRÍTICO: Solo procesar archivos completos, nunca partes fragmentadas
            if not self._is_complete_video_file(file_path):
                return False
                
            return True
            
        except Exception as e:
            logger.debug(f"Error checking file {file_path}: {e}")
            return False
    
    def _is_complete_video_file(self, file_path: str) -> bool:
        """
        Determina si un archivo MP4 es un video completo listo para upload.
        
        Criterios:
        1. Archivos _COMPLETE.mp4: Siempre son resultado final de concatenación
        2. Videos simples sin fragmentación: No tienen cont_X y no hay otras partes
        3. RECHAZA: part_X.mp4, cont_X.mp4 (fragmentos intermedios)
        
        Returns:
            bool: True si es un video completo listo para upload
        """
        try:
            filename = os.path.basename(file_path)
            video_dir = os.path.dirname(file_path)
            
            # REGLA 1: Archivos _COMPLETE.mp4 siempre son finales
            if "_COMPLETE.mp4" in filename:
                logger.debug(f"✅ Complete video detected: {filename}")
                return True
            
            # REGLA 2: Rechazar fragmentos conocidos
            if self._is_fragmented_file(filename):
                logger.debug(f"❌ Fragmented file detected, skipping: {filename}")
                return False
            
            # REGLA 3: Para videos simples, verificar que no hay fragmentación en el directorio
            if self._has_fragmentation_in_directory(file_path):
                logger.debug(f"❌ Directory has fragmentation, skipping simple video: {filename}")
                return False
            
            # REGLA 4: Es un video simple completo
            logger.debug(f"✅ Simple complete video detected: {filename}")
            return True
            
        except Exception as e:
            logger.debug(f"Error checking if video is complete {file_path}: {e}")
            return False
    
    def _is_fragmented_file(self, filename: str) -> bool:
        """
        Detecta si un archivo es un fragmento (part_X, cont_X)
        """
        # Patrones de fragmentación conocidos
        fragmentation_patterns = [
            "_part",     # TK_user_part1.mp4, TK_user_part2.mp4
            "_cont",     # TK_user_cont2.mp4, TK_user_cont3.mp4
            "cont2",     # TK_user_cont2.mp4 (sin guión bajo)
            "cont3",     # TK_user_cont3.mp4 (sin guión bajo)
            "cont4",     # etc.
        ]
        
        filename_lower = filename.lower()
        for pattern in fragmentation_patterns:
            if pattern in filename_lower:
                return True
        
        return False
    
    def _has_fragmentation_in_directory(self, file_path: str) -> bool:
        """
        Verifica si el directorio contiene otros fragmentos del mismo usuario/timestamp
        que sugieran que la grabación aún está siendo procesada
        """
        try:
            video_dir = os.path.dirname(file_path)
            filename = os.path.basename(file_path)
            
            # Extraer usuario y timestamp del archivo actual
            # Patrón: TK_username_timestamp.mp4
            pattern = r"TK_([^_]+)_(\d{4}\.\d{2}\.\d{2}_\d{2}-\d{2}-\d{2})"
            match = re.match(pattern, filename)
            
            if not match:
                return False
                
            username = match.group(1)
            timestamp = match.group(2)
            base_pattern = f"TK_{username}_{timestamp}"
            
            # Buscar otros archivos con el mismo patrón base
            try:
                for file_in_dir in os.listdir(video_dir):
                    if file_in_dir == filename:  # Skip el archivo actual
                        continue
                        
                    if file_in_dir.startswith(base_pattern) and file_in_dir.endswith('.mp4'):
                        # Si hay archivos _COMPLETE, significa que la concatenación ya terminó
                        if "_COMPLETE.mp4" in file_in_dir:
                            logger.debug(f"Found COMPLETE version, simple video should be skipped: {filename}")
                            return True
                            
                        # Si hay fragmentos (cont_, part_), significa fragmentación en curso
                        if self._is_fragmented_file(file_in_dir):
                            logger.debug(f"Found fragmentation in directory for {username}: {file_in_dir}")
                            return True
                
                return False
                
            except OSError as e:
                logger.debug(f"Could not list directory {video_dir}: {e}")
                return False
                
        except Exception as e:
            logger.debug(f"Error checking directory fragmentation: {e}")
            return False
    
    def _has_pending_collage(self, mp4_file: str) -> bool:
        """
        Verifica si el archivo tiene un collage asociado que sugiere procesamiento incompleto
        """
        try:
            video_dir = os.path.dirname(mp4_file)
            video_name = os.path.splitext(os.path.basename(mp4_file))[0]
            
            # Buscar collage correspondiente
            collage_pattern = os.path.join(video_dir, f"{video_name}_collage.jpg")
            return os.path.exists(collage_pattern)
            
        except Exception:
            return False
    
    def _extract_file_metadata(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Extrae metadata del archivo para determinar usuario y configuración
        Busca primero en recording_states para obtener chat_id
        """
        try:
            filename = os.path.basename(file_path)
            
            # Patrón: TK_username_timestamp.mp4, TK_username_timestamp_COMPLETE.mp4, TK_username_timestamp_contX.mp4, etc.
            pattern = r"TK_([^_]+)_(\d{4}\.\d{2}\.\d{2}_\d{2}-\d{2}-\d{2})(?:_COMPLETE|_cont(\d+)|_part(\d+))?\.mp4"
            match = re.match(pattern, filename)
            
            if not match:
                logger.debug(f"Filename doesn't match expected pattern: {filename}")
                return None
                
            username = match.group(1)
            timestamp = match.group(2)
            cont_number = int(match.group(3)) if match.group(3) else None
            part_number = int(match.group(4)) if match.group(4) else None
            
            # Para archivos _COMPLETE, siempre usar part_number = 1 (video final)
            if "_COMPLETE.mp4" in filename:
                final_part_number = 1
                is_fragmented = True  # Indica que es el resultado de fragmentación
            elif cont_number or part_number:
                # Para fragmentos, usar el número apropiado
                final_part_number = max(cont_number or 1, part_number or 1)
                is_fragmented = True
            else:
                # Videos simples
                final_part_number = 1
                is_fragmented = False
            
            # CRÍTICO: Buscar chat_id en recording_states
            chat_id = self._find_chat_id_from_session(username, timestamp)
            
            if chat_id is None:
                logger.warning(f"⚠️ Could not find chat_id for {username} - file may not be recoverable: {filename}")
                return None
            
            # Buscar archivos asociados
            collage_path = self._find_associated_collage(file_path)
            thumbnail_path = self._find_associated_thumbnail(file_path)
            
            file_size = os.path.getsize(file_path)
            
            return {
                'file_path': file_path,
                'username': username,
                'chat_id': chat_id,  # AÑADIDO: chat_id desde session state
                'timestamp': timestamp,
                'part_number': final_part_number,
                'is_fragmented': is_fragmented,
                'file_size': file_size,
                'collage_path': collage_path,
                'thumbnail_path': thumbnail_path,
                'recovery_time': time.time()
            }
            
        except Exception as e:
            logger.error(f"Error extracting metadata from {file_path}: {e}")
            return None
    
    def _find_associated_collage(self, mp4_file: str) -> Optional[str]:
        """Busca el collage asociado al video"""
        try:
            video_dir = os.path.dirname(mp4_file)
            video_name = os.path.splitext(os.path.basename(mp4_file))[0]
            
            collage_path = os.path.join(video_dir, f"{video_name}_collage.jpg")
            return collage_path if os.path.exists(collage_path) else None
            
        except Exception:
            return None
    
    def _find_associated_thumbnail(self, mp4_file: str) -> Optional[str]:
        """Busca el thumbnail asociado al video"""
        try:
            video_dir = os.path.dirname(mp4_file)
            video_name = os.path.splitext(os.path.basename(mp4_file))[0]
            
            thumb_path = os.path.join(video_dir, f"{video_name}_thumb.jpg")
            return thumb_path if os.path.exists(thumb_path) else None
            
        except Exception:
            return None
    
    def _find_chat_id_from_session(self, username: str, timestamp: str) -> Optional[int]:
        """
        Busca el chat_id en los archivos de recording_states
        basándose en username y timestamp del archivo
        """
        try:
            # Directorio de estados de sesión
            state_dir = os.path.join(self.base_directory, "recording_states")
            
            if not os.path.exists(state_dir):
                logger.debug(f"Recording states directory not found: {state_dir}")
                return None
            
            # Buscar archivos de sesión que coincidan
            for filename in os.listdir(state_dir):
                if not filename.startswith("session_") or not filename.endswith(".json"):
                    continue
                    
                session_file = os.path.join(state_dir, filename)
                
                try:
                    with open(session_file, 'r', encoding='utf-8') as f:
                        session_data = json.load(f)
                    
                    # Verificar si es la sesión correcta
                    if session_data.get('user') == username:
                        # Verificar timestamp aproximado
                        session_start = session_data.get('start_time', 0)
                        
                        # Convertir timestamp del archivo a epoch time para comparar
                        try:
                            from datetime import datetime
                            # Parse "2025.07.17_22-12-55" format
                            clean_timestamp = timestamp.replace('.', '-').replace('_', ' ').replace('-', ':')
                            file_time = datetime.strptime(clean_timestamp, "%Y:%m:%d %H:%M:%S").timestamp()
                            
                            # Si está dentro de una ventana de tiempo razonable (1 hora)
                            time_diff = abs(file_time - session_start)
                            if time_diff < 3600:
                                chat_id = session_data.get('telegram_chat_id')
                                if chat_id:
                                    logger.info(f"🔗 Found chat_id {chat_id} for {username} in session {session_data.get('session_id')}")
                                    return int(chat_id)
                        except Exception as e:
                            logger.debug(f"Error parsing timestamp {timestamp}: {e}")
                            continue
                    
                except Exception as e:
                    logger.debug(f"Error reading session file {session_file}: {e}")
                    continue
            
            logger.warning(f"⚠️ Could not find matching session for {username} with timestamp {timestamp}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error searching for chat_id in sessions: {e}")
            return None
    
    def _get_recovery_timestamp(self, file_path: str) -> Optional[float]:
        """Obtiene el timestamp del último intento de recovery"""
        return self.recovery_timestamps.get(file_path)
    
    def mark_file_as_recovered(self, file_path: str):
        """Marca un archivo como recuperado para evitar reprocesarlo"""
        current_time = time.time()
        self.recovered_files.add(file_path)
        self.recovery_timestamps[file_path] = current_time
        self._save_recovery_state()
        
    def _load_recovery_state(self) -> set:
        """Carga el estado de archivos ya recuperados"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    return set(data.get('recovered_files', []))
        except Exception as e:
            logger.debug(f"Could not load recovery state: {e}")
        return set()
    
    def _load_recovery_timestamps(self) -> dict:
        """Carga los timestamps de recovery"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    return data.get('recovery_timestamps', {})
        except Exception as e:
            logger.debug(f"Could not load recovery timestamps: {e}")
        return {}
    
    def _save_recovery_state(self):
        """Guarda el estado de archivos recuperados"""
        try:
            state_data = {
                'last_updated': time.time(),
                'recovered_files': list(self.recovered_files),
                'recovery_timestamps': self.recovery_timestamps
            }
            
            with open(self.state_file, 'w') as f:
                json.dump(state_data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Could not save recovery state: {e}")
    
    def cleanup_old_recovery_state(self, days: int = 7):
        """Limpia entradas antiguas del estado de recuperación"""
        try:
            # Eliminar archivos que ya no existen del estado
            existing_files = {f for f in self.recovered_files if os.path.exists(f)}
            
            if len(existing_files) != len(self.recovered_files):
                removed_count = len(self.recovered_files) - len(existing_files)
                logger.info(f"🧹 Cleaned up {removed_count} non-existent files from recovery state")
                self.recovered_files = existing_files
                self._save_recovery_state()
                
        except Exception as e:
            logger.error(f"Error during recovery state cleanup: {e}")


# Función de conveniencia para usar desde otros módulos
def scan_and_recover_pending_uploads() -> List[Dict[str, Any]]:
    """
    Función de conveniencia para escanear y obtener archivos pendientes de upload
    """
    scanner = UploadRecoveryScanner()
    return scanner.scan_for_pending_uploads()


def mark_file_recovered(file_path: str):
    """
    Función de conveniencia para marcar un archivo como recuperado
    """
    scanner = UploadRecoveryScanner()
    scanner.mark_file_as_recovered(file_path)


def clear_recovery_state():
    """
    Función de conveniencia para limpiar el estado de recovery (solo para debugging)
    """
    scanner = UploadRecoveryScanner()
    scanner.recovered_files.clear()
    scanner.recovery_timestamps.clear()
    scanner._save_recovery_state()
    logger.info("🧹 Recovery state cleared for debugging")