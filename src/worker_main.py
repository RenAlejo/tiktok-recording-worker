#!/usr/bin/env python3
"""
TikTok Recording Worker
Processes recording jobs from Redis queue independently from the bot
"""

import sys
import os
import asyncio
import platform
import signal
import time
import socket
import threading
import uuid
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.utils import banner
from utils.dependencies import check_and_install_dependencies
from utils.logger_manager import logger
from config.env_config import config
from services.work_queue_service import (
    WorkQueueService, RecordingJob, JobType, JobStatus, WorkerInfo,
    work_queue_service
)
from services.worker_recording_service import WorkerRecordingService
# Temporary: Upload imports disabled for Redis-first implementation
# from upload.upload_manager import UploadManager
# from upload.telegram import Telegram
from utils.enums import Mode


class RecordingWorker:
    """
    Independent recording worker that processes jobs from Redis queue
    """
    
    def __init__(self, worker_id: str = None):
        self.worker_id = worker_id or f"worker_{socket.gethostname()}_{int(time.time())}"
        self.hostname = socket.gethostname()
        self.ip_address = self._get_local_ip()
        self.work_queue = work_queue_service
        self.recording_service = None
        self.upload_manager = None
        self.telegram_uploader = None
        self.running = False
        self.current_jobs = {}
        self.heartbeat_thread = None
        
        # Worker info
        self.worker_info = WorkerInfo(
            worker_id=self.worker_id,
            hostname=self.hostname,
            ip_address=self.ip_address,
            status="available",
            active_jobs=0,
            max_concurrent_jobs=config.worker_max_concurrent_jobs,
            capabilities=["recording", "upload"]
        )
    
    def _get_local_ip(self) -> str:
        """Get local IP address"""
        try:
            # Connect to external address to determine local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except:
            return "127.0.0.1"
    
    async def initialize(self):
        """Initialize worker components with async context"""
        try:
            logger.info(f"🚀 Initializing recording worker {self.worker_id}")
            
            # Initialize Telegram uploader (restored from original functionality)
            try:
                # Read telegram configuration
                from utils.utils import read_telegram_config
                from upload.telegram import Telegram
                from upload.upload_manager import UploadManager
                from pyrogram import Client
                
                telegram_config = read_telegram_config()
                
                # Initialize Pyrogram client
                telegram_client = Client(
                    'worker_telegram_session',
                    api_id=telegram_config["api_id"], 
                    api_hash=telegram_config["api_hash"],
                    bot_token=telegram_config["bot_token"],
                    max_concurrent_transmissions=config.upload_max_concurrent_transmissions
                )
                
                # CRITICAL: Start Telegram client first (same as original project)
                await telegram_client.start()
                logger.info("✅ Telegram client connected successfully")
                
                # Initialize Telegram uploader
                self.telegram_uploader = Telegram(telegram_client)
                
                # Initialize recording service FIRST (required for UploadManager singleton access)
                self.recording_service = WorkerRecordingService(telegram_uploader=self.telegram_uploader, worker_id=self.worker_id)
                logger.info("✅ WorkerRecordingService singleton registered for UploadManager")

                # Start post-processing queue (critical for processing recordings)
                if self.recording_service.postprocessing_queue:
                    await self.recording_service.postprocessing_queue.start()
                    logger.info("✅ Post-processing queue started")
                else:
                    logger.warning("⚠️ Post-processing queue not available")

                # Initialize upload manager AFTER recording service
                self.upload_manager = UploadManager()
                self.upload_manager.set_telegram_instance(self.telegram_uploader)
                
                # Start upload manager workers (critical for queue processing)
                await self.upload_manager.start()
                
                logger.info("✅ Telegram uploader and upload manager initialized successfully")
                
            except Exception as e:
                logger.warning(f"⚠️ Telegram initialization failed: {e}")
                logger.warning("⚠️ Worker will continue without upload functionality")
                self.telegram_uploader = None
                self.upload_manager = None
                # Still initialize recording service even without Telegram
                self.recording_service = WorkerRecordingService(telegram_uploader=None, worker_id=self.worker_id)
                logger.info("✅ WorkerRecordingService initialized without Telegram uploader")

                # Start post-processing queue even without Telegram
                if self.recording_service.postprocessing_queue:
                    await self.recording_service.postprocessing_queue.start()
                    logger.info("✅ Post-processing queue started (without Telegram)")
            
            # Register worker in Redis
            success = self.work_queue.register_worker(self.worker_info)
            if not success:
                raise Exception("Failed to register worker in Redis")
            
            logger.info(f"✅ Worker {self.worker_id} initialized successfully")
            logger.info(f"   Hostname: {self.hostname}")
            logger.info(f"   IP: {self.ip_address}")
            logger.info(f"   Max concurrent jobs: {self.worker_info.max_concurrent_jobs}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize worker: {e}")
            raise
    
    def start_heartbeat(self):
        """Start heartbeat thread"""
        def heartbeat_loop():
            while self.running:
                try:
                    # Send heartbeat with recording service data for unified system
                    self.work_queue.worker_heartbeat(self.worker_id, self.recording_service)
                    time.sleep(config.worker_heartbeat_interval_seconds)
                except Exception as e:
                    logger.error(f"❌ Heartbeat error: {e}")
                    time.sleep(5)  # Shorter retry interval on error
        
        self.heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        logger.info(f"💓 Heartbeat started for worker {self.worker_id}")
    
    async def run(self):
        """Main worker loop (async for proper event loop context)"""
        try:
            self.running = True
            self.start_heartbeat()
            
            logger.info(f"🎬 Worker {self.worker_id} started processing jobs")
            
            while self.running:
                try:
                    # Check if we can take more jobs
                    if len(self.current_jobs) >= self.worker_info.max_concurrent_jobs:
                        logger.debug(f"Worker {self.worker_id} at max capacity, waiting...")
                        await asyncio.sleep(10)
                        continue
                    
                    # Try to get a job
                    job = self.work_queue.dequeue_job(self.worker_id)
                    if job is None:
                        # No jobs available, wait and try again
                        await asyncio.sleep(5)
                        continue
                    
                    # Process job in separate thread
                    job_thread = threading.Thread(
                        target=self._process_job,
                        args=(job,),
                        daemon=True
                    )
                    job_thread.start()
                    
                    # Track the job
                    self.current_jobs[job.job_id] = {
                        'job': job,
                        'thread': job_thread,
                        'started_at': time.time()
                    }
                    
                    # Update worker status
                    self.worker_info.active_jobs = len(self.current_jobs)
                    self.worker_info.status = "busy" if len(self.current_jobs) > 0 else "available"
                    
                except KeyboardInterrupt:
                    logger.info("🛑 Worker shutdown requested")
                    break
                except Exception as e:
                    logger.error(f"❌ Error in worker main loop: {e}")
                    await asyncio.sleep(5)
            
        except Exception as e:
            logger.error(f"❌ Fatal error in worker: {e}")
        finally:
            await self.shutdown()
    
    def _process_job(self, job: RecordingJob):
        """Process a single recording job"""
        try:
            logger.info(f"🎥 Processing job {job.job_id} for {job.username}")
            
            if job.job_type == JobType.RECORDING_REQUEST:
                self._process_recording_job(job)
            elif job.job_type == JobType.STOP_RECORDING:
                self._process_stop_job(job)
            else:
                logger.warning(f"⚠️ Unknown job type: {job.job_type}")
                self.work_queue.update_job_status(job.job_id, JobStatus.FAILED, {
                    "error": f"Unknown job type: {job.job_type}"
                })
        
        except Exception as e:
            logger.error(f"❌ Error processing job {job.job_id}: {e}")
            self.work_queue.update_job_status(job.job_id, JobStatus.FAILED, {
                "error": str(e),
                "error_type": type(e).__name__
            })
        finally:
            # Remove from current jobs
            if job.job_id in self.current_jobs:
                del self.current_jobs[job.job_id]
            
            # Update worker status
            self.worker_info.active_jobs = len(self.current_jobs)
            self.worker_info.status = "busy" if len(self.current_jobs) > 0 else "available"
    
    def _process_recording_job(self, job: RecordingJob):
        """Process a recording request job"""
        try:
            # Update job status to in progress
            self.work_queue.update_job_status(job.job_id, JobStatus.IN_PROGRESS, {
                "worker_id": self.worker_id,
                "started_at": datetime.now().isoformat()
            })
            
            # Update recording status - started
            self.work_queue.update_recording_status(job.username, {
                "status": "recording",
                "job_id": job.job_id,
                "worker_id": self.worker_id,
                "started_at": time.time()
            })
            
            # Convert job mode to Mode enum
            mode = Mode.MANUAL  # Default
            if hasattr(Mode, job.mode.upper()):
                mode = getattr(Mode, job.mode.upper())
            
            # Start recording using existing recording service (funciona exactamente igual que antes)
            result = self.recording_service.start_recording(
                username=job.username,
                user_id=job.user_id,
                chat_id=job.chat_id,
                language=job.language,
                mode=mode,
                job_id=job.job_id  # Pass job_id for real-time notifications
            )
            
            # Update job status to completed
            self.work_queue.update_job_status(job.job_id, JobStatus.COMPLETED, {
                "completed_at": datetime.now().isoformat(),
                "result": str(result)
            })
            
            # Update recording status - recording active (job completed means recording started)
            self.work_queue.update_recording_status(job.username, {
                "status": "recording",  # Recording is now active
                "job_id": job.job_id,
                "worker_id": self.worker_id,
                "recording_started_at": time.time()
            })
            
            logger.info(f"✅ Recording job {job.job_id} completed for {job.username}")
            
        except Exception as e:
            logger.error(f"❌ Recording job {job.job_id} failed: {e}")
            
            # Update job status to failed
            self.work_queue.update_job_status(job.job_id, JobStatus.FAILED, {
                "failed_at": datetime.now().isoformat(),
                "error": str(e),
                "error_type": type(e).__name__
            })
            
            # Update recording status - failed
            self.work_queue.update_recording_status(job.username, {
                "status": "failed",
                "job_id": job.job_id,
                "worker_id": self.worker_id,
                "error": str(e),
                "failed_at": time.time()
            })
            
            raise
    
    def _process_stop_job(self, job: RecordingJob):
        """Process a stop recording job"""
        try:
            # Update job status
            self.work_queue.update_job_status(job.job_id, JobStatus.IN_PROGRESS, {
                "worker_id": self.worker_id,
                "started_at": datetime.now().isoformat()
            })
            
            # Stop recording using local recording service
            result = self.recording_service.stop_recording(job.username, job.user_id)
            
            # Update job status to completed
            self.work_queue.update_job_status(job.job_id, JobStatus.COMPLETED, {
                "completed_at": datetime.now().isoformat(),
                "result": str(result)
            })
            
            # Update recording status
            self.work_queue.update_recording_status(job.username, {
                "status": "stopped",
                "stopped_at": time.time()
            })
            
            logger.info(f"✅ Stop job {job.job_id} completed for {job.username}")
            
        except Exception as e:
            logger.error(f"❌ Stop job {job.job_id} failed: {e}")
            
            # Update job status to failed
            self.work_queue.update_job_status(job.job_id, JobStatus.FAILED, {
                "failed_at": datetime.now().isoformat(),
                "error": str(e),
                "error_type": type(e).__name__
            })
            
            raise
    
    async def shutdown(self):
        """Shutdown worker gracefully"""
        logger.info(f"🔄 Shutting down worker {self.worker_id}")
        self.running = False
        
        # Stop post-processing queue first
        if self.recording_service and self.recording_service.postprocessing_queue:
            try:
                await self.recording_service.postprocessing_queue.stop()
                logger.info("✅ Post-processing queue stopped")
            except Exception as e:
                logger.warning(f"⚠️ Error stopping post-processing queue: {e}")

        # Stop upload manager
        if self.upload_manager:
            try:
                await self.upload_manager.stop()
                logger.info("✅ Upload manager stopped")
            except Exception as e:
                logger.warning(f"⚠️ Error stopping upload manager: {e}")

        # Stop Telegram client
        if self.telegram_uploader and hasattr(self.telegram_uploader, 'app'):
            try:
                await self.telegram_uploader.app.stop()
                logger.info("✅ Telegram client disconnected")
            except Exception as e:
                logger.warning(f"⚠️ Error stopping Telegram client: {e}")
        
        # Wait for current jobs to complete (with timeout)
        if self.current_jobs:
            logger.info(f"⏳ Waiting for {len(self.current_jobs)} jobs to complete...")
            timeout = 60  # 1 minute timeout
            start_time = time.time()
            
            while self.current_jobs and (time.time() - start_time) < timeout:
                time.sleep(1)
            
            if self.current_jobs:
                logger.warning(f"⚠️ {len(self.current_jobs)} jobs still running after timeout")
        
        logger.info(f"✅ Worker {self.worker_id} shutdown complete")


# Global worker instance for signal handler access
_worker_instance = None

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"📡 Received signal {signum}, initiating shutdown...")
    if _worker_instance:
        _worker_instance.running = False
        logger.info("🛑 Worker shutdown flag set")
    else:
        logger.warning("⚠️ No worker instance available for shutdown")
    
    # Exit immediately for repeated signals
    import sys
    sys.exit(0)


async def main():
    """Main entry point with proper asyncio context"""
    # Show banner
    banner()
    print(f"🤖 TikTok Recording Worker")
    print(f"   Mode: Independent Worker")
    print(f"   Redis: {config.redis_url}")
    print("=" * 50)
    
    # Check dependencies
    check_and_install_dependencies()
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run worker
    global _worker_instance
    worker = RecordingWorker()
    _worker_instance = worker  # Make accessible to signal handler
    
    try:
        await worker.initialize()
        await worker.run()  # Now fully async for proper event loop context
    except KeyboardInterrupt:
        logger.info("🛑 Worker interrupted by user")
    except Exception as e:
        logger.error(f"❌ Worker error: {e}")
    finally:
        await worker.shutdown()


if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Worker stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal worker error: {e}")