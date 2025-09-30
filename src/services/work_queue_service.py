"""
Work Queue Service for Bot-Worker Communication
Manages job distribution and worker coordination via Redis queues
"""

import redis
import json
import time
import uuid
import threading
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum

from utils.logger_manager import logger
from config.env_config import config


class JobStatus(Enum):
    """Job status enumeration"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobType(Enum):
    """Job type enumeration"""
    RECORDING_REQUEST = "recording_request"
    STOP_RECORDING = "stop_recording"
    STATUS_CHECK = "status_check"


@dataclass
class RecordingJob:
    """Recording job data structure"""
    job_id: str
    job_type: JobType
    username: str
    user_id: int
    chat_id: int
    language: str
    mode: str = "live"
    priority: int = 1
    created_at: float = None
    assigned_worker: Optional[str] = None
    status: JobStatus = JobStatus.PENDING
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()
        if self.metadata is None:
            self.metadata = {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Redis storage"""
        data = asdict(self)
        data['job_type'] = self.job_type.value
        data['status'] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RecordingJob':
        """Create from dictionary loaded from Redis"""
        data['job_type'] = JobType(data['job_type'])
        data['status'] = JobStatus(data['status'])
        return cls(**data)


@dataclass
class WorkerInfo:
    """Worker information structure"""
    worker_id: str
    hostname: str
    ip_address: str
    status: str = "available"  # available, busy, offline
    last_heartbeat: float = None
    active_jobs: int = 0
    max_concurrent_jobs: int = 5
    capabilities: List[str] = None

    def __post_init__(self):
        if self.last_heartbeat is None:
            self.last_heartbeat = time.time()
        if self.capabilities is None:
            self.capabilities = ["recording", "upload"]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Redis storage"""
        data = asdict(self)
        # Convert list to JSON string for Redis storage
        if isinstance(data.get('capabilities'), list):
            data['capabilities'] = json.dumps(data['capabilities'])
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkerInfo':
        """Create from dictionary loaded from Redis"""
        # Convert JSON string back to list
        if isinstance(data.get('capabilities'), str):
            data['capabilities'] = json.loads(data['capabilities'])
        # Convert string numbers back to proper types
        if 'last_heartbeat' in data and isinstance(data['last_heartbeat'], str):
            data['last_heartbeat'] = float(data['last_heartbeat'])
        if 'active_jobs' in data and isinstance(data['active_jobs'], str):
            data['active_jobs'] = int(data['active_jobs'])
        if 'max_concurrent_jobs' in data and isinstance(data['max_concurrent_jobs'], str):
            data['max_concurrent_jobs'] = int(data['max_concurrent_jobs'])
        return cls(**data)


class WorkQueueService:
    """
    Singleton service for managing work queues and worker coordination
    
    Redis Keys Structure:
    - job_queue:{priority}: Sorted sets for job queues by priority
    - job:{job_id}: Job data storage
    - worker:{worker_id}: Worker information
    - workers:heartbeat: Workers last seen timestamps
    - recording_status:{username}: Real-time recording status
    - user_jobs:{user_id}: Active jobs per user
    """

    _instance: Optional['WorkQueueService'] = None
    _initialized: bool = False
    _lock = threading.Lock()

    # Redis key patterns
    JOB_QUEUE_KEY = "job_queue:{priority}"
    JOB_DATA_KEY = "job:{job_id}"
    WORKER_INFO_KEY = "worker:{worker_id}"
    WORKER_HEARTBEAT_KEY = "workers:heartbeat"
    RECORDING_STATUS_KEY = "recording_status:{username}"
    USER_JOBS_KEY = "user_jobs:{user_id}"
    GLOBAL_STATS_KEY = "system:stats"

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            try:
                # Redis connection - optimized for Upstash cloud
                self.redis = redis.from_url(
                    config.redis_url,
                    decode_responses=True,
                    socket_connect_timeout=30,  # Increased for Upstash
                    socket_timeout=60,  # Increased for Upstash cloud latency
                    socket_keepalive=True,  # Enable keepalive for cloud Redis
                    socket_keepalive_options={},
                    retry_on_timeout=True,
                    health_check_interval=30,  # Check connection health
                    max_connections=config.redis_max_connections
                )

                # Test connection
                self.redis.ping()
                logger.info("✅ WorkQueueService Redis connection established")

                self._initialized = True

            except Exception as e:
                logger.error(f"❌ Failed to initialize WorkQueueService Redis connection: {e}")
                raise

    # Job Management Methods

    def get_best_worker_for_job(self, job: RecordingJob) -> Optional[str]:
        """
        Encuentra el mejor worker para un job usando balanceado de carga inteligente

        Algoritmo:
        1. Obtener todos los workers activos (heartbeat reciente)
        2. Filtrar workers con capacidad disponible
        3. Calcular score de carga (% utilización)
        4. Seleccionar worker con menor carga relativa

        Args:
            job: Job a asignar

        Returns:
            worker_id del mejor worker o None si no hay workers disponibles
        """
        try:
            current_time = time.time()
            heartbeat_timeout = config.worker_heartbeat_timeout_seconds

            # Obtener todos los workers con heartbeat reciente
            active_workers = []
            worker_keys = self.redis.keys(f"{self.WORKER_KEY.format(worker_id='*')}")

            for worker_key in worker_keys:
                try:
                    worker_data = self.redis.hgetall(worker_key)
                    if not worker_data:
                        continue

                    # Verificar heartbeat reciente
                    last_heartbeat = float(worker_data.get('last_heartbeat', 0))
                    if current_time - last_heartbeat > heartbeat_timeout:
                        continue  # Worker offline

                    worker_id = worker_data.get('worker_id')
                    active_jobs = int(worker_data.get('active_jobs', 0))
                    max_jobs = int(worker_data.get('max_concurrent_jobs', 1))

                    # Solo considerar workers con capacidad disponible
                    if active_jobs < max_jobs:
                        load_percentage = (active_jobs / max_jobs) * 100
                        active_workers.append({
                            'worker_id': worker_id,
                            'active_jobs': active_jobs,
                            'max_jobs': max_jobs,
                            'load_percentage': load_percentage,
                            'available_slots': max_jobs - active_jobs
                        })

                except Exception as worker_error:
                    logger.debug(f"Error processing worker data: {worker_error}")
                    continue

            if not active_workers:
                logger.debug("No workers available for job assignment")
                return None

            # Ordenar por carga (menor carga primero)
            # En caso de empate, priorizar por más slots disponibles
            active_workers.sort(key=lambda w: (w['load_percentage'], -w['available_slots']))

            best_worker = active_workers[0]

            logger.debug(f"📊 Selected worker {best_worker['worker_id']} "
                        f"(load: {best_worker['load_percentage']:.1f}%, "
                        f"jobs: {best_worker['active_jobs']}/{best_worker['max_jobs']})")

            return best_worker['worker_id']

        except Exception as e:
            logger.error(f"❌ Error finding best worker for job: {e}")
            return None

    def enqueue_job(self, job: RecordingJob) -> bool:
        """
        Enqueue a job with intelligent load balancing assignment

        Args:
            job: RecordingJob instance

        Returns:
            bool: True if successfully enqueued
        """
        try:
            # NUEVO: Asignación dirigida con balanceado de carga
            assigned_worker = self.get_best_worker_for_job(job)

            if assigned_worker:
                # Asignar job directamente al worker seleccionado
                job.assigned_worker = assigned_worker
                return self._enqueue_job_to_worker(job, assigned_worker)
            else:
                # Fallback: usar cola tradicional si no hay workers disponibles
                return self._enqueue_job_to_general_queue(job)

        except Exception as e:
            logger.error(f"❌ Failed to enqueue job {job.job_id}: {e}")
            return False

    def _enqueue_job_to_worker(self, job: RecordingJob, worker_id: str) -> bool:
        """
        Asigna un job directamente a un worker específico
        """
        try:
            pipe = self.redis.pipeline()

            # Store job data
            job_key = self.JOB_DATA_KEY.format(job_id=job.job_id)
            pipe.hset(job_key, mapping=job.to_dict())
            pipe.expire(job_key, config.job_data_expiry_seconds)

            # NUEVO: Agregar a cola específica del worker
            worker_queue_key = f"worker_queue:{worker_id}"
            pipe.zadd(worker_queue_key, {job.job_id: job.created_at})
            pipe.expire(worker_queue_key, config.job_data_expiry_seconds)

            # Track user jobs
            user_jobs_key = self.USER_JOBS_KEY.format(user_id=job.user_id)
            pipe.sadd(user_jobs_key, job.job_id)
            pipe.expire(user_jobs_key, config.user_jobs_expiry_seconds)

            # Update global stats
            pipe.hincrby(self.GLOBAL_STATS_KEY, "jobs_enqueued", 1)
            pipe.hincrby(self.GLOBAL_STATS_KEY, "jobs_pending", 1)

            pipe.execute()

            logger.info(f"📤 Job {job.job_id} assigned to worker {worker_id} for {job.username} (priority: {job.priority})")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to assign job {job.job_id} to worker {worker_id}: {e}")
            return False

    def _enqueue_job_to_general_queue(self, job: RecordingJob) -> bool:
        """
        Fallback: agrega job a cola general (comportamiento original)
        """
        try:
            pipe = self.redis.pipeline()

            # Store job data
            job_key = self.JOB_DATA_KEY.format(job_id=job.job_id)
            pipe.hset(job_key, mapping=job.to_dict())
            pipe.expire(job_key, config.job_data_expiry_seconds)

            # Add to priority queue (comportamiento original)
            queue_key = self.JOB_QUEUE_KEY.format(priority=job.priority)
            pipe.zadd(queue_key, {job.job_id: job.created_at})

            # Track user jobs
            user_jobs_key = self.USER_JOBS_KEY.format(user_id=job.user_id)
            pipe.sadd(user_jobs_key, job.job_id)
            pipe.expire(user_jobs_key, config.user_jobs_expiry_seconds)

            # Update global stats
            pipe.hincrby(self.GLOBAL_STATS_KEY, "jobs_enqueued", 1)
            pipe.hincrby(self.GLOBAL_STATS_KEY, "jobs_pending", 1)

            pipe.execute()

            logger.info(f"📤 Job {job.job_id} enqueued to general queue for {job.username} (no workers available)")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to enqueue job {job.job_id}: {e}")
            return False

    def dequeue_job(self, worker_id: str, priorities: List[int] = None) -> Optional[RecordingJob]:
        """
        Dequeue next available job for a worker with load-balanced assignment support

        Args:
            worker_id: ID of the worker requesting job
            priorities: List of priorities to check (default: [1, 2, 3])

        Returns:
            RecordingJob or None if no jobs available
        """
        if priorities is None:
            priorities = [1, 2, 3]  # High to low priority

        try:
            # PRIORIDAD 1: Verificar cola específica del worker (asignación dirigida)
            job = self._dequeue_from_worker_queue(worker_id)
            if job:
                return job

            # PRIORIDAD 2: Buscar en colas generales (fallback para compatibilidad)
            job = self._dequeue_from_general_queues(worker_id, priorities)
            if job:
                return job

            return None  # No jobs available

        except Exception as e:
            logger.error(f"❌ Failed to dequeue job for worker {worker_id}: {e}")
            return None

    def _dequeue_from_worker_queue(self, worker_id: str) -> Optional[RecordingJob]:
        """
        Busca jobs en la cola específica del worker (asignación dirigida)
        """
        try:
            worker_queue_key = f"worker_queue:{worker_id}"

            # Get oldest job from worker's specific queue
            job_ids = self.redis.zrange(worker_queue_key, 0, 0)
            if not job_ids:
                return None

            job_id = job_ids[0]

            # Remove from queue atomically
            removed = self.redis.zrem(worker_queue_key, job_id)
            if not removed:
                return None  # Job was already taken

            # Get and process job
            job = self._get_and_process_job(job_id, worker_id)
            if job:
                logger.info(f"📥 Job {job_id} taken from worker-specific queue by {worker_id}")

            return job

        except Exception as e:
            logger.debug(f"Error checking worker queue for {worker_id}: {e}")
            return None

    def _dequeue_from_general_queues(self, worker_id: str, priorities: List[int]) -> Optional[RecordingJob]:
        """
        Busca jobs en colas generales (comportamiento original para fallback)
        """
        try:
            for priority in priorities:
                queue_key = self.JOB_QUEUE_KEY.format(priority=priority)

                # Get oldest job from this priority queue
                job_ids = self.redis.zrange(queue_key, 0, 0)
                if not job_ids:
                    continue

                job_id = job_ids[0]

                # Remove from queue atomically
                removed = self.redis.zrem(queue_key, job_id)
                if not removed:
                    continue  # Job was taken by another worker

                # Get and process job
                job = self._get_and_process_job(job_id, worker_id)
                if job:
                    logger.info(f"📥 Job {job_id} taken from general queue (priority {priority}) by {worker_id}")
                    return job

            return None

        except Exception as e:
            logger.debug(f"Error checking general queues for {worker_id}: {e}")
            return None

    def _get_and_process_job(self, job_id: str, worker_id: str) -> Optional[RecordingJob]:
        """
        Obtiene datos del job y lo procesa para asignación
        """
        try:
            # Get job data
            job_key = self.JOB_DATA_KEY.format(job_id=job_id)
            job_data = self.redis.hgetall(job_key)

            if not job_data:
                logger.warning(f"⚠️ Job {job_id} data not found, skipping")
                return None

            # Parse job
            job = RecordingJob.from_dict(job_data)
            job.assigned_worker = worker_id
            job.status = JobStatus.IN_PROGRESS

            # Update job status
            pipe = self.redis.pipeline()
            pipe.hset(job_key, mapping=job.to_dict())
            pipe.hincrby(self.GLOBAL_STATS_KEY, "jobs_pending", -1)
            pipe.hincrby(self.GLOBAL_STATS_KEY, "jobs_in_progress", 1)
            pipe.execute()

            return job

        except Exception as e:
            logger.debug(f"Error processing job {job_id} for worker {worker_id}: {e}")
            return None

    def update_job_status(self, job_id: str, status: JobStatus, metadata: Dict[str, Any] = None) -> bool:
        """
        Update job status and metadata
        
        Args:
            job_id: Job identifier
            status: New job status
            metadata: Optional metadata to update
            
        Returns:
            bool: True if successfully updated
        """
        try:
            job_key = self.JOB_DATA_KEY.format(job_id=job_id)
            
            pipe = self.redis.pipeline()
            pipe.hset(job_key, "status", status.value)
            
            if metadata:
                for key, value in metadata.items():
                    pipe.hset(job_key, f"metadata.{key}", json.dumps(value))
            
            # Update global stats
            if status == JobStatus.COMPLETED:
                pipe.hincrby(self.GLOBAL_STATS_KEY, "jobs_in_progress", -1)
                pipe.hincrby(self.GLOBAL_STATS_KEY, "jobs_completed", 1)
            elif status == JobStatus.FAILED:
                pipe.hincrby(self.GLOBAL_STATS_KEY, "jobs_in_progress", -1)
                pipe.hincrby(self.GLOBAL_STATS_KEY, "jobs_failed", 1)
            
            pipe.execute()
            
            logger.info(f"📝 Job {job_id} status updated to {status.value}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update job {job_id} status: {e}")
            return False

    # Worker Management Methods

    def register_worker(self, worker_info: WorkerInfo) -> bool:
        """
        Register a worker in the system
        
        Args:
            worker_info: Worker information
            
        Returns:
            bool: True if successfully registered
        """
        try:
            worker_key = self.WORKER_INFO_KEY.format(worker_id=worker_info.worker_id)
            
            pipe = self.redis.pipeline()
            pipe.hset(worker_key, mapping=worker_info.to_dict())
            pipe.expire(worker_key, config.worker_info_expiry_seconds)
            
            # Update heartbeat
            pipe.hset(self.WORKER_HEARTBEAT_KEY, worker_info.worker_id, time.time())
            
            # Update stats
            pipe.hincrby(self.GLOBAL_STATS_KEY, "workers_registered", 1)
            
            pipe.execute()
            
            logger.info(f"🤖 Worker {worker_info.worker_id} registered from {worker_info.hostname}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to register worker {worker_info.worker_id}: {e}")
            return False

    def worker_heartbeat(self, worker_id: str, recording_service=None) -> bool:
        """
        Update worker heartbeat with scalable individual keys (optimized for hundreds of workers)

        Args:
            worker_id: Worker identifier
            recording_service: Optional recording service for recording data

        Returns:
            bool: True if successfully updated
        """
        try:
            current_time = time.time()

            # LEGACY: Keep basic heartbeat for compatibility (will be deprecated)
            self.redis.hset(self.WORKER_HEARTBEAT_KEY, worker_id, current_time)

            # SCALABLE: Individual worker heartbeat key with detailed data
            if recording_service:
                try:
                    recordings_data = recording_service.get_heartbeat_recordings()

                    # Extract region from worker_id (format: worker_region_datacenter_001)
                    worker_parts = worker_id.split('_')
                    region = worker_parts[1] if len(worker_parts) > 1 else "default"

                    # Scalable heartbeat data structure
                    heartbeat_data = {
                        "timestamp": str(current_time),
                        "worker_id": worker_id,
                        "region": region,
                        "status": "active",

                        # Capacity information for load balancing
                        "capacity_max": str(recordings_data.get("capacity", {}).get("max_concurrent", 5)),
                        "capacity_current": str(recordings_data.get("capacity", {}).get("current_active", 0)),
                        "capacity_available": str(recordings_data.get("capacity", {}).get("available_slots", 5)),
                        "capacity_utilization": str(recordings_data.get("capacity", {}).get("utilization_percent", 0)),

                        # Performance metrics
                        "performance_success_rate": str(recordings_data.get("performance", {}).get("success_rate", 100.0)),
                        "performance_avg_duration": str(recordings_data.get("performance", {}).get("avg_recording_duration", 1800)),

                        # System metrics for monitoring
                        "system_cpu": str(recordings_data.get("system_info", {}).get("cpu_usage_percent", 0)),
                        "system_memory": str(recordings_data.get("system_info", {}).get("memory_usage_percent", 0)),
                        "system_memory_available": str(recordings_data.get("system_info", {}).get("memory_available_gb", 0)),

                        # Recording data (compressed)
                        "active_recordings_count": str(len(recordings_data.get("active_recordings", []))),
                        "master_recordings_count": str(recordings_data.get("master_recordings_count", 0)),

                        # Detailed recordings (only for monitoring, not load balancing)
                        "active_recordings_detail": json.dumps(recordings_data.get("active_recordings", []))
                    }

                    # Use individual key for scalability: worker:{region}:{worker_id}:heartbeat
                    heartbeat_key = f"worker:{region}:{worker_id}:heartbeat"

                    # Atomic operation with TTL for automatic cleanup
                    pipe = self.redis.pipeline()
                    pipe.hset(heartbeat_key, mapping=heartbeat_data)
                    pipe.expire(heartbeat_key, 45)  # 45 second TTL (worker sends every 30s)

                    # Also update regional metrics for aggregated monitoring
                    regional_key = f"region:{region}:workers"
                    pipe.zadd(regional_key, {worker_id: current_time})
                    pipe.expire(regional_key, 300)  # 5 minute TTL for regional tracking

                    pipe.execute()

                    logger.debug(f"💓 Worker {worker_id} heartbeat updated: {recordings_data.get('capacity', {}).get('current_active', 0)} active, "
                               f"{recordings_data.get('capacity', {}).get('available_slots', 0)} available slots")

                except Exception as e:
                    logger.error(f"❌ Error updating detailed heartbeat for {worker_id}: {e}")
                    # Fall back to basic heartbeat only
                    pass
            else:
                logger.warning(f"🔍 DEBUG: No recording service provided for {worker_id}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Failed to update heartbeat for worker {worker_id}: {e}")
            return False

    def get_available_workers(self, max_age_seconds: int = 60) -> List[WorkerInfo]:
        """
        Get list of available workers (based on recent heartbeat)
        
        Args:
            max_age_seconds: Maximum age of heartbeat to consider worker alive
            
        Returns:
            List of available WorkerInfo objects
        """
        try:
            current_time = time.time()
            cutoff_time = current_time - max_age_seconds
            
            # Get recent heartbeats
            heartbeats = self.redis.hgetall(self.WORKER_HEARTBEAT_KEY)
            active_workers = []
            
            for worker_id, last_seen in heartbeats.items():
                if float(last_seen) >= cutoff_time:
                    # Get worker info
                    worker_key = self.WORKER_INFO_KEY.format(worker_id=worker_id)
                    worker_data = self.redis.hgetall(worker_key)
                    
                    if worker_data:
                        worker_info = WorkerInfo.from_dict(worker_data)
                        if worker_info.status == "available":
                            active_workers.append(worker_info)
            
            return active_workers
            
        except Exception as e:
            logger.error(f"❌ Failed to get available workers: {e}")
            return []

    # Recording Status Methods

    def update_recording_status(self, username: str, status_data: Dict[str, Any]) -> bool:
        """
        Update real-time recording status
        
        Args:
            username: TikTok username being recorded
            status_data: Status information
            
        Returns:
            bool: True if successfully updated
        """
        try:
            status_key = self.RECORDING_STATUS_KEY.format(username=username)
            
            # Add timestamp
            status_data['updated_at'] = time.time()
            
            pipe = self.redis.pipeline()
            pipe.hset(status_key, mapping=status_data)
            pipe.expire(status_key, config.recording_status_expiry_seconds)
            pipe.execute()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update recording status for {username}: {e}")
            return False

    def get_recording_status(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Get current recording status for username
        
        Args:
            username: TikTok username
            
        Returns:
            Dict with status data or None if not found
        """
        try:
            status_key = self.RECORDING_STATUS_KEY.format(username=username)
            return self.redis.hgetall(status_key) or None
        except Exception as e:
            logger.error(f"❌ Failed to get recording status for {username}: {e}")
            return None

    # Utility Methods

    def get_user_active_jobs(self, user_id: int) -> List[str]:
        """
        Get list of active job IDs for a user
        
        Args:
            user_id: Telegram user ID
            
        Returns:
            List of job IDs
        """
        try:
            user_jobs_key = self.USER_JOBS_KEY.format(user_id=user_id)
            return list(self.redis.smembers(user_jobs_key))
        except Exception as e:
            logger.error(f"❌ Failed to get active jobs for user {user_id}: {e}")
            return []

    def cleanup_expired_jobs(self, max_age_hours: int = 24) -> int:
        """
        Cleanup expired jobs and orphaned data
        
        Args:
            max_age_hours: Maximum age of jobs to keep
            
        Returns:
            int: Number of jobs cleaned up
        """
        try:
            cutoff_time = time.time() - (max_age_hours * 3600)
            cleaned_count = 0
            
            # Clean up job queues
            for priority in [1, 2, 3]:
                queue_key = self.JOB_QUEUE_KEY.format(priority=priority)
                removed = self.redis.zremrangebyscore(queue_key, 0, cutoff_time)
                cleaned_count += removed
            
            logger.info(f"🧹 Cleaned up {cleaned_count} expired jobs")
            return cleaned_count
            
        except Exception as e:
            logger.error(f"❌ Failed to cleanup expired jobs: {e}")
            return 0

    def get_system_stats(self) -> Dict[str, Any]:
        """
        Get system statistics
        
        Returns:
            Dict with system stats
        """
        try:
            stats = self.redis.hgetall(self.GLOBAL_STATS_KEY)
            
            # Add real-time metrics
            available_workers = len(self.get_available_workers())
            stats['workers_available'] = available_workers
            
            # Queue lengths
            total_pending = 0
            for priority in [1, 2, 3]:
                queue_key = self.JOB_QUEUE_KEY.format(priority=priority)
                queue_length = self.redis.zcard(queue_key)
                stats[f'queue_priority_{priority}'] = queue_length
                total_pending += queue_length
            
            stats['jobs_total_pending'] = total_pending
            
            return stats

        except Exception as e:
            logger.error(f"❌ Failed to get system stats: {e}")
            return {}

    def get_load_balancing_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas específicas de balanceado de carga

        Returns:
            Dict con estadísticas detalladas de distribución de workers
        """
        try:
            current_time = time.time()
            heartbeat_timeout = config.worker_heartbeat_timeout_seconds

            # Obtener información de todos los workers
            workers = []
            total_active_jobs = 0
            total_capacity = 0

            worker_keys = self.redis.keys(f"{self.WORKER_KEY.format(worker_id='*')}")

            for worker_key in worker_keys:
                try:
                    worker_data = self.redis.hgetall(worker_key)
                    if not worker_data:
                        continue

                    # Verificar heartbeat reciente
                    last_heartbeat = float(worker_data.get('last_heartbeat', 0))
                    if current_time - last_heartbeat > heartbeat_timeout:
                        continue  # Worker offline

                    worker_id = worker_data.get('worker_id')
                    active_jobs = int(worker_data.get('active_jobs', 0))
                    max_jobs = int(worker_data.get('max_concurrent_jobs', 1))
                    load_percentage = (active_jobs / max_jobs) * 100

                    workers.append({
                        'worker_id': worker_id,
                        'active_jobs': active_jobs,
                        'max_jobs': max_jobs,
                        'load_percentage': load_percentage,
                        'available_slots': max_jobs - active_jobs
                    })

                    total_active_jobs += active_jobs
                    total_capacity += max_jobs

                except Exception as worker_error:
                    logger.debug(f"Error processing worker data for load balancing stats: {worker_error}")
                    continue

            # Calcular estadísticas de distribución
            if workers:
                loads = [w['load_percentage'] for w in workers]
                avg_load = sum(loads) / len(loads)
                load_variance = sum((load - avg_load) ** 2 for load in loads) / len(loads)
                load_std_dev = load_variance ** 0.5

                # Contar workers por rango de carga
                load_ranges = {
                    'idle': len([w for w in workers if w['load_percentage'] == 0]),
                    'low': len([w for w in workers if 0 < w['load_percentage'] <= 25]),
                    'medium': len([w for w in workers if 25 < w['load_percentage'] <= 75]),
                    'high': len([w for w in workers if 75 < w['load_percentage'] < 100]),
                    'full': len([w for w in workers if w['load_percentage'] == 100])
                }
            else:
                avg_load = 0
                load_std_dev = 0
                load_ranges = {'idle': 0, 'low': 0, 'medium': 0, 'high': 0, 'full': 0}

            # Contar jobs en colas específicas de workers
            worker_queues_count = 0
            for worker in workers:
                worker_queue_key = f"worker_queue:{worker['worker_id']}"
                worker_queues_count += self.redis.zcard(worker_queue_key)

            return {
                'timestamp': current_time,
                'workers': {
                    'total_online': len(workers),
                    'total_active_jobs': total_active_jobs,
                    'total_capacity': total_capacity,
                    'utilization_percentage': (total_active_jobs / total_capacity * 100) if total_capacity > 0 else 0
                },
                'load_distribution': {
                    'average_load_percentage': round(avg_load, 2),
                    'load_standard_deviation': round(load_std_dev, 2),
                    'is_balanced': load_std_dev < 20,  # Considerar balanceado si std dev < 20%
                    'ranges': load_ranges
                },
                'queue_distribution': {
                    'worker_specific_queues': worker_queues_count,
                    'general_queue_priority_1': self.redis.zcard(self.JOB_QUEUE_KEY.format(priority=1)),
                    'general_queue_priority_2': self.redis.zcard(self.JOB_QUEUE_KEY.format(priority=2)),
                    'general_queue_priority_3': self.redis.zcard(self.JOB_QUEUE_KEY.format(priority=3))
                },
                'workers_detail': workers[:10]  # Limitar a 10 workers para evitar logs enormes
            }

        except Exception as e:
            logger.error(f"❌ Failed to get load balancing stats: {e}")
            return {'error': str(e)}


# Global instance
work_queue_service = WorkQueueService()