"""
Monitor de recursos del sistema para prevenir errores como "Too many open files"
Monitorea file descriptors, memoria y otros recursos críticos
"""

import os
import time
import psutil
import threading
import subprocess
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from utils.logger_manager import logger
from config.env_config import config


@dataclass
class LeakSnapshot:
    """Snapshot for leak detection"""
    timestamp: float
    file_descriptors: int
    tcp_connections: int
    close_wait_connections: int
    fifo_pipes: int
    active_recordings: int


class ResourceMonitor:
    """
    Monitor de recursos del sistema que alerta cuando se acercan los límites
    y toma acciones preventivas para evitar errores críticos
    """
    
    def __init__(self):
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.current_process = psutil.Process()
        self._stop_event = threading.Event()
        
        # Leak detection
        self.leak_snapshots: List[LeakSnapshot] = []
        self.baseline_fds: Optional[int] = None
        self.last_leak_check = 0.0
        
    def start_monitoring(self):
        """Inicia el monitoreo de recursos en background"""
        if not config.enable_resource_monitoring:
            logger.debug("Resource monitoring disabled by configuration")
            return
            
        if self.monitoring:
            logger.debug("Resource monitoring already running")
            return
            
        self.monitoring = True
        self._stop_event.clear()
        
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            name="ResourceMonitor",
            daemon=True
        )
        self.monitor_thread.start()
        
        logger.info("🔍 Resource monitoring started")
    
    def stop_monitoring(self):
        """Detiene el monitoreo de recursos"""
        if self.monitoring:
            self.monitoring = False
            self._stop_event.set()
            
            if self.monitor_thread and self.monitor_thread.is_alive():
                self.monitor_thread.join(timeout=5)
                
            logger.info("🔍 Resource monitoring stopped")
    
    def _monitoring_loop(self):
        """Loop principal de monitoreo"""
        logger.info("🔍 Resource monitoring loop started")
        
        while self.monitoring and not self._stop_event.is_set():
            try:
                self._check_file_descriptors()
                self._check_memory_usage()
                self._check_disk_space()
                
                # Check for leaks every 5 minutes
                current_time = time.time()
                if current_time - self.last_leak_check >= 300:  # 5 minutes
                    self._check_resource_leaks()
                    self.last_leak_check = current_time
                
                # Pausa entre checks (30 segundos)
                if self._stop_event.wait(timeout=30):
                    break
                    
            except Exception as e:
                logger.error(f"Error in resource monitoring: {e}")
                time.sleep(30)
        
        logger.debug("🔍 Resource monitoring loop ended")
    
    def _check_file_descriptors(self):
        """Monitorea el uso de file descriptors"""
        try:
            # Obtener número actual de FDs
            fd_count = self.current_process.num_fds()
            
            # Obtener límites del proceso con múltiples métodos
            soft_limit = 2000  # Default conservative
            
            # Método 0: Configuración manual (más alta prioridad)
            if config.system_file_descriptors_limit > 0:
                soft_limit = config.system_file_descriptors_limit
                logger.debug(f"Using manual config limit: {soft_limit}")
            else:
                try:
                    # Método 1: Usar resource module (más confiable)
                    import resource
                    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                    if soft > 1024:  # Si es mayor a default, usarlo
                        soft_limit = soft
                        logger.debug(f"Using resource module limit: {soft_limit}")
                    else:
                        # Método 2: /proc/PID/limits como fallback
                        limits_file = f"/proc/{self.current_process.pid}/limits"
                        if os.path.exists(limits_file):
                            with open(limits_file, 'r') as f:
                                for line in f:
                                    if 'open files' in line:
                                        parts = line.split()
                                        proc_limit = int(parts[3]) if parts[3] != 'unlimited' else 65536
                                        if proc_limit > soft_limit:
                                            soft_limit = proc_limit
                                            logger.debug(f"Using /proc/limits: {soft_limit}")
                                        break
                except Exception as e:
                    logger.debug(f"Error getting FD limits, using default: {e}")
                    soft_limit = 2000
            
            # Calcular porcentaje de uso
            usage_percent = (fd_count / soft_limit) * 100
            
            # Alertas basadas en configuración
            if fd_count >= config.max_file_descriptors_critical:
                logger.error(f"🚨 CRITICAL: File descriptors usage: {fd_count}/{soft_limit} ({usage_percent:.1f}%)")
                self._handle_critical_fd_usage(fd_count, soft_limit)
                
            elif fd_count >= config.max_file_descriptors_warning:
                logger.warning(f"⚠️ WARNING: File descriptors usage: {fd_count}/{soft_limit} ({usage_percent:.1f}%)")
                self._handle_warning_fd_usage(fd_count, soft_limit)
                
            else:
                # Log normal cada 5 minutos (10 checks)
                if hasattr(self, '_fd_log_counter'):
                    self._fd_log_counter += 1
                else:
                    self._fd_log_counter = 1
                    
                if self._fd_log_counter >= 10:
                    logger.info(f"📊 File descriptors usage: {fd_count}/{soft_limit} ({usage_percent:.1f}%)")
                    self._fd_log_counter = 0
                    
        except Exception as e:
            logger.debug(f"Error checking file descriptors: {e}")
    
    def _check_memory_usage(self):
        """Monitorea el uso de memoria del proceso"""
        try:
            memory_info = self.current_process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            
            # Alert si usa más de 2GB
            if memory_mb > 2048:
                logger.warning(f"⚠️ High memory usage: {memory_mb:.1f}MB")
                
            # Log normal cada 10 minutos (20 checks)
            if hasattr(self, '_mem_log_counter'):
                self._mem_log_counter += 1
            else:
                self._mem_log_counter = 1
                
            if self._mem_log_counter >= 20:
                logger.info(f"📊 Memory usage: {memory_mb:.1f}MB")
                self._mem_log_counter = 0
                
        except Exception as e:
            logger.debug(f"Error checking memory usage: {e}")
    
    def _check_disk_space(self):
        """Monitorea el espacio disponible en disco"""
        try:
            disk_usage = psutil.disk_usage(config.output_directory)
            free_gb = disk_usage.free / (1024**3)
            total_gb = disk_usage.total / (1024**3)
            used_percent = (disk_usage.used / disk_usage.total) * 100
            
            # Alert si queda menos de 5GB
            if free_gb < 5:
                logger.error(f"🚨 LOW DISK SPACE: {free_gb:.1f}GB free ({used_percent:.1f}% used)")
                
            elif free_gb < 10:
                logger.warning(f"⚠️ Disk space warning: {free_gb:.1f}GB free ({used_percent:.1f}% used)")
                
            # Log normal cada 15 minutos (30 checks)
            if hasattr(self, '_disk_log_counter'):
                self._disk_log_counter += 1
            else:
                self._disk_log_counter = 1
                
            if self._disk_log_counter >= 30:
                logger.info(f"📊 Disk usage: {free_gb:.1f}GB free / {total_gb:.1f}GB total ({used_percent:.1f}% used)")
                self._disk_log_counter = 0
                
        except Exception as e:
            logger.debug(f"Error checking disk space: {e}")
    
    def _handle_critical_fd_usage(self, current: int, limit: int):
        """Maneja uso crítico de file descriptors"""
        try:
            logger.error(f"🚨 Taking emergency action for critical FD usage: {current}/{limit}")
            
            # 1. Forzar garbage collection
            import gc
            collected = gc.collect()
            logger.info(f"🧹 Forced garbage collection: {collected} objects collected")
            
            # 2. Intentar aumentar límite si es posible
            try:
                import resource
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                if soft < hard:
                    new_limit = min(hard, soft * 2)
                    resource.setrlimit(resource.RLIMIT_NOFILE, (new_limit, hard))
                    logger.info(f"🔧 Increased file descriptor limit: {soft} -> {new_limit}")
            except Exception as e:
                logger.debug(f"Could not increase FD limit: {e}")
            
        except Exception as e:
            logger.error(f"Error handling critical FD usage: {e}")
    
    def _handle_warning_fd_usage(self, current: int, limit: int):
        """Maneja uso alto de file descriptors"""
        try:
            logger.warning(f"⚠️ High FD usage detected: {current}/{limit}")
            
            # Forzar garbage collection preventivo
            import gc
            collected = gc.collect()
            if collected > 0:
                logger.info(f"🧹 Preventive garbage collection: {collected} objects collected")
                
        except Exception as e:
            logger.debug(f"Error handling warning FD usage: {e}")
    
    def get_current_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas actuales de recursos"""
        try:
            stats = {
                'file_descriptors': {
                    'current': self.current_process.num_fds(),
                    'warning_threshold': config.max_file_descriptors_warning,
                    'critical_threshold': config.max_file_descriptors_critical
                },
                'memory': {
                    'rss_mb': self.current_process.memory_info().rss / (1024 * 1024),
                    'vms_mb': self.current_process.memory_info().vms / (1024 * 1024)
                },
                'disk': {},
                'monitoring_active': self.monitoring
            }
            
            # Disk info
            disk_usage = psutil.disk_usage(config.output_directory)
            stats['disk'] = {
                'free_gb': disk_usage.free / (1024**3),
                'total_gb': disk_usage.total / (1024**3),
                'used_percent': (disk_usage.used / disk_usage.total) * 100
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting resource stats: {e}")
            return {'error': str(e)}
    
    def _check_resource_leaks(self, active_recordings: int = 0):
        """Detecta posibles fugas de recursos"""
        try:
            # Get current resource usage
            fd_count = self.current_process.num_fds()
            tcp_connections = len([c for c in self.current_process.connections() if c.type == psutil.CONN_TCP])
            
            # Get CLOSE_WAIT and FIFO counts (Linux only)
            close_wait_count = 0
            fifo_count = 0
            
            if os.name != 'nt':  # Unix systems
                try:
                    # CLOSE_WAIT connections from netstat
                    result = subprocess.run(
                        ['netstat', '-an'], 
                        capture_output=True, 
                        text=True, 
                        timeout=10
                    )
                    if result.returncode == 0:
                        close_wait_count = result.stdout.count('CLOSE_WAIT')
                    
                    # FIFO pipes from lsof
                    lsof_result = subprocess.run(
                        ['lsof', '-p', str(self.current_process.pid)], 
                        capture_output=True, 
                        text=True, 
                        timeout=10
                    )
                    if lsof_result.returncode == 0:
                        fifo_count = lsof_result.stdout.count('FIFO')
                    
                except (subprocess.TimeoutExpired, subprocess.SubprocessError, FileNotFoundError):
                    pass
            
            # Create snapshot
            snapshot = LeakSnapshot(
                timestamp=time.time(),
                file_descriptors=fd_count,
                tcp_connections=tcp_connections,
                close_wait_connections=close_wait_count,
                fifo_pipes=fifo_count,
                active_recordings=active_recordings
            )
            
            self.leak_snapshots.append(snapshot)
            
            # Keep only last 24 snapshots (2 hours worth at 5-minute intervals)
            if len(self.leak_snapshots) > 24:
                self.leak_snapshots = self.leak_snapshots[-24:]
            
            # Set baseline if first snapshot
            if self.baseline_fds is None:
                self.baseline_fds = fd_count
                logger.info(f"🔍 Resource leak detection baseline set: {fd_count} FDs")
                return
            
            # Detect leaks if we have enough data
            if len(self.leak_snapshots) >= 3:
                self._analyze_leaks()
            
            # Log comprehensive summary every hour (12 snapshots)
            if len(self.leak_snapshots) % 12 == 0:
                self._log_resource_summary(snapshot)
                
        except Exception as e:
            logger.debug(f"Error checking resource leaks: {e}")
    
    def _analyze_leaks(self):
        """Analiza las tendencias para detectar fugas"""
        if len(self.leak_snapshots) < 3:
            return
        
        current = self.leak_snapshots[-1]
        baseline = self.leak_snapshots[0]
        time_diff_hours = (current.timestamp - baseline.timestamp) / 3600
        
        if time_diff_hours <= 0:
            return
        
        # Calculate growth rates per hour
        fd_growth = (current.file_descriptors - baseline.file_descriptors) / time_diff_hours
        tcp_growth = (current.tcp_connections - baseline.tcp_connections) / time_diff_hours
        
        leaks_detected = []
        
        # File descriptor leak (>10 FDs/hour sustained growth)
        if fd_growth > 10:
            fd_increase = current.file_descriptors - self.baseline_fds
            leaks_detected.append(f"File descriptor leak: {fd_growth:.1f} FDs/hour (+{fd_increase} total)")
        
        # TCP connection leak (>5 connections/hour)
        if tcp_growth > 5:
            leaks_detected.append(f"TCP connection leak: {tcp_growth:.1f} connections/hour")
        
        # Absolute thresholds
        if current.close_wait_connections > 500:
            leaks_detected.append(f"Excessive CLOSE_WAIT connections: {current.close_wait_connections}")
        
        if current.fifo_pipes > 500:
            leaks_detected.append(f"Excessive FIFO pipes: {current.fifo_pipes}")
        
        # Log detected leaks
        if leaks_detected:
            logger.error(f"🚨 RESOURCE LEAKS DETECTED:")
            for leak in leaks_detected:
                logger.error(f"   - {leak}")
            logger.error(f"   Active recordings: {current.active_recordings}")
            logger.error(f"   Time window: {time_diff_hours:.1f} hours")
    
    def _log_resource_summary(self, snapshot: LeakSnapshot):
        """Log comprehensive resource usage summary"""
        logger.info(f"📊 RESOURCE SUMMARY (Active recordings: {snapshot.active_recordings})")
        logger.info(f"   File Descriptors: {snapshot.file_descriptors}")
        if self.baseline_fds:
            increase = snapshot.file_descriptors - self.baseline_fds
            logger.info(f"   FD Increase: {increase:+d} from baseline")
        logger.info(f"   TCP Connections: {snapshot.tcp_connections}")
        logger.info(f"   CLOSE_WAIT: {snapshot.close_wait_connections}")
        logger.info(f"   FIFO Pipes: {snapshot.fifo_pipes}")
    
    def force_leak_check(self, active_recordings: int = 0):
        """Force an immediate leak check (for testing/debugging)"""
        self._check_resource_leaks(active_recordings)
    
    def get_leak_report(self) -> Dict[str, Any]:
        """Get detailed leak detection report"""
        if not self.leak_snapshots:
            return {'status': 'no_data'}
        
        current = self.leak_snapshots[-1]
        report = {
            'status': 'monitoring',
            'current': {
                'file_descriptors': current.file_descriptors,
                'tcp_connections': current.tcp_connections,
                'close_wait_connections': current.close_wait_connections,
                'fifo_pipes': current.fifo_pipes,
                'active_recordings': current.active_recordings
            },
            'baseline_fds': self.baseline_fds,
            'snapshots_count': len(self.leak_snapshots)
        }
        
        if self.baseline_fds:
            report['fd_increase'] = current.file_descriptors - self.baseline_fds
        
        return report


# Instancia global del monitor
resource_monitor = ResourceMonitor()