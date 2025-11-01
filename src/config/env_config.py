"""
Configuration module for reading environment variables with default values
"""
import os
from typing import Union, Optional
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class EnvConfig:
    """Central configuration class for environment variables"""
    
    @staticmethod
    def get_bool(key: str, default: bool = False) -> bool:
        """Get boolean value from environment variable"""
        value = os.getenv(key, str(default)).lower()
        return value in ('true', '1', 'yes', 'on')
    
    @staticmethod
    def get_int(key: str, default: int) -> int:
        """Get integer value from environment variable"""
        try:
            return int(os.getenv(key, str(default)))
        except ValueError:
            return default
    
    @staticmethod
    def get_float(key: str, default: float) -> float:
        """Get float value from environment variable"""
        try:
            return float(os.getenv(key, str(default)))
        except ValueError:
            return default
    
    @staticmethod
    def get_str(key: str, default: str = "") -> str:
        """Get string value from environment variable"""
        return os.getenv(key, default)
    
    # Recording Configuration
    @property
    def output_directory(self) -> str:
        return self.get_str("OUTPUT_DIRECTORY", "D:/recordings")
    
    @property
    def enable_organized_folders(self) -> bool:
        return self.get_bool("ENABLE_ORGANIZED_FOLDERS", True)
    
    @property
    def max_recordings_per_user(self) -> int:
        return self.get_int("MAX_RECORDINGS_PER_USER", 2)
    
    @property
    def max_server_recordings(self) -> int:
        return self.get_int("MAX_SERVER_RECORDINGS", 60)
    
    @property
    def automatic_interval_minutes(self) -> int:
        return self.get_int("AUTOMATIC_INTERVAL_MINUTES", 5)
    
    # File Management
    @property
    def max_file_size_bytes(self) -> int:
        size_gb = self.get_float("MAX_FILE_SIZE_GB", 1.5)
        return int(size_gb * 1024 * 1024 * 1024)
    
    @property
    def size_check_interval_seconds(self) -> int:
        return self.get_int("SIZE_CHECK_INTERVAL_SECONDS", 5)
    
    @property
    def buffer_size_bytes(self) -> int:
        size_kb = self.get_int("BUFFER_SIZE_KB", 512)
        return size_kb * 1024
    
    @property
    def fragmentation_safety_margin_percent(self) -> int:
        return self.get_int("FRAGMENTATION_SAFETY_MARGIN_PERCENT", 5)
    
    # HTTP & Proxy Configuration
    @property
    def enable_proxy(self) -> bool:
        return self.get_bool("ENABLE_PROXY", True)
    
    @property
    def http_request_timeout(self) -> int:
        return self.get_int("HTTP_REQUEST_TIMEOUT_SECONDS", 8)
    
    @property
    def http_connect_timeout(self) -> int:
        return self.get_int("HTTP_CONNECT_TIMEOUT_SECONDS", 5)
    
    @property
    def max_retries(self) -> int:
        return self.get_int("MAX_RETRIES", 10)
    
    @property
    def retry_backoff_factor(self) -> float:
        return self.get_float("RETRY_BACKOFF_FACTOR", 2.0)
    
    # Cache Configuration
    @property
    def user_cache_ttl_minutes(self) -> int:
        return self.get_int("USER_CACHE_TTL_MINUTES", 30)
    
    @property
    def cache_max_retries(self) -> int:
        return self.get_int("CACHE_MAX_RETRIES", 3)
    
    @property
    def escalating_ttl_enabled(self) -> bool:
        return self.get_bool("ESCALATING_TTL_ENABLED", True)
    
    @property
    def enable_room_id_cache(self) -> bool:
        return self.get_bool("ENABLE_ROOM_ID_CACHE", True)
    
    # API Configuration
    @property
    def tiktok_api_base_url(self) -> str:
        return self.get_str("TIKTOK_API_BASE_URL", "https://www.tiktok.com")
    
    @property
    def tiktoklive_session_id(self) -> str:
        return self.get_str("TIKTOKLIVE_SESSION_ID", "")
    
    @property
    def tiktoklive_api_key(self) -> str:
        return self.get_str("TIKTOKLIVE_API_KEY", "")
    
    @property
    def tiktok_live_base_url(self) -> str:
        return self.get_str("TIKTOK_LIVE_BASE_URL", "https://webcast.tiktok.com")
    
    @property
    def api_request_delay_ms(self) -> int:
        return self.get_int("API_REQUEST_DELAY_MS", 1000)
    
    # Bot Configuration
    @property
    def bot_status_check_interval(self) -> int:
        return self.get_int("BOT_STATUS_CHECK_INTERVAL_SECONDS", 2)
    
    @property
    def bot_max_wait_seconds(self) -> int:
        return self.get_int("BOT_MAX_WAIT_SECONDS", 60)
    
    @property
    def bot_recording_init_wait(self) -> int:
        return self.get_int("BOT_RECORDING_INIT_WAIT_SECONDS", 5)

    # Fragment retry configuration
    @property
    def fragment_url_retry_attempts(self) -> int:
        """Number of retry attempts to get stream URL for fragments (no delays between retries)"""
        return self.get_int("FRAGMENT_URL_RETRY_ATTEMPTS", 10)

    # Progress Messages Configuration
    @property
    def progress_validating_user_duration(self) -> int:
        return self.get_int("PROGRESS_VALIDATING_USER_DURATION", 8)
    
    @property
    def progress_extracting_info_duration(self) -> int:
        return self.get_int("PROGRESS_EXTRACTING_INFO_DURATION", 12)
    
    @property
    def progress_checking_live_duration(self) -> int:
        return self.get_int("PROGRESS_CHECKING_LIVE_DURATION", 10)
    
    @property
    def progress_connecting_duration(self) -> int:
        return self.get_int("PROGRESS_CONNECTING_DURATION", 15)
    
    @property
    def progress_starting_recording_duration(self) -> int:
        return self.get_int("PROGRESS_STARTING_RECORDING_DURATION", 15)
    
    # Upload Configuration
    @property
    def upload_queue_enabled(self) -> bool:
        return self.get_bool("UPLOAD_QUEUE_ENABLED", True)
    
    @property
    def upload_retry_attempts(self) -> int:
        return self.get_int("UPLOAD_RETRY_ATTEMPTS", 3)
    
    @property
    def upload_chunk_size_mb(self) -> int:
        return self.get_int("UPLOAD_CHUNK_SIZE_MB", 50)
    
    @property
    def max_upload_queue_size(self) -> int:
        return self.get_int("MAX_UPLOAD_QUEUE_SIZE", 30)
    
    @property
    def upload_queue_capacity_threshold_percent(self) -> int:
        return self.get_int("UPLOAD_QUEUE_CAPACITY_THRESHOLD_PERCENT", 55)
    
    # Weighted Round-Robin Upload Configuration
    @property
    def upload_large_file_threshold_mb(self) -> int:
        return self.get_int("UPLOAD_LARGE_FILE_THRESHOLD_MB", 600)
    
    @property
    def upload_small_to_large_ratio(self) -> int:
        return self.get_int("UPLOAD_SMALL_TO_LARGE_RATIO", 7)
    
    @property
    def upload_aging_threshold_minutes(self) -> int:
        return self.get_int("UPLOAD_AGING_THRESHOLD_MINUTES", 5)
    
    @property
    def upload_emergency_threshold_minutes(self) -> int:
        return self.get_int("UPLOAD_EMERGENCY_THRESHOLD_MINUTES", 15)
    
    # Upload Concurrency Configuration
    @property
    def upload_concurrent_workers(self) -> int:
        return self.get_int("UPLOAD_CONCURRENT_WORKERS", 2)
    
    @property
    def upload_max_concurrent_transmissions(self) -> int:
        return self.get_int("UPLOAD_MAX_CONCURRENT_TRANSMISSIONS", 3)
    
    # Upload Delay Configuration
    @property
    def upload_delay_between_tasks_seconds(self) -> int:
        return self.get_int("UPLOAD_DELAY_BETWEEN_TASKS_SECONDS", 8)
    
    @property
    def upload_delay_after_error_seconds(self) -> int:
        return self.get_int("UPLOAD_DELAY_AFTER_ERROR_SECONDS", 20)
    
    @property
    def upload_delay_large_files_seconds(self) -> int:
        return self.get_int("UPLOAD_DELAY_LARGE_FILES_SECONDS", 12)
    
    @property
    def upload_delay_enabled(self) -> bool:
        return self.get_bool("UPLOAD_DELAY_ENABLED", True)
    
    # Backup Bot Configuration
    @property
    def enable_backup_bot(self) -> bool:
        return self.get_bool("ENABLE_BACKUP_BOT", False)
    
    @property
    def backup_bot_token(self) -> str:
        return self.get_str("BACKUP_BOT_TOKEN", "")
    
    @property
    def backup_bot_api_id(self) -> str:
        return self.get_str("BACKUP_BOT_API_ID", "")
    
    @property
    def backup_bot_api_hash(self) -> str:
        return self.get_str("BACKUP_BOT_API_HASH", "")
    
    @property
    def backup_group_chat_id(self) -> str:
        return self.get_str("BACKUP_GROUP_CHAT_ID", "")

    @property
    def backup_group_chat_id_2(self) -> str:
        return self.get_str("BACKUP_GROUP_CHAT_ID2", "")

    # Message Forwarding for Backup (simplified - no separate bot needed)
    @property
    def enable_backup_forwarding(self) -> bool:
        return self.get_bool("ENABLE_BACKUP_FORWARDING", False)
    
    # Logging Configuration
    @property
    def log_level(self) -> str:
        return self.get_str("LOG_LEVEL", "INFO")
    
    @property
    def log_file_prefix(self) -> str:
        return self.get_str("LOG_FILE_PREFIX", "bot_log")
    
    @property
    def log_rotation_days(self) -> int:
        return self.get_int("LOG_ROTATION_DAYS", 7)
    
    # Maintenance Configuration
    @property
    def maintenance_mode(self) -> bool:
        return self.get_bool("MAINTENANCE_MODE", False)
    
    @property
    def maintenance_check_interval(self) -> int:
        return self.get_int("MAINTENANCE_CHECK_INTERVAL_SECONDS", 300)
    
    # Username Validation
    @property
    def username_min_length(self) -> int:
        return self.get_int("USERNAME_MIN_LENGTH", 2)
    
    @property
    def username_max_length(self) -> int:
        return self.get_int("USERNAME_MAX_LENGTH", 24)
    
    @property
    def strict_username_validation(self) -> bool:
        return self.get_bool("STRICT_USERNAME_VALIDATION", True)
    
    # Stream Corruption Handling Configuration
    @property
    def enable_corruption_detection(self) -> bool:
        return self.get_bool("ENABLE_CORRUPTION_DETECTION", True)
    
    @property
    def max_corruption_duration_seconds(self) -> int:
        return self.get_int("MAX_CORRUPTION_DURATION_SECONDS", 30)
    
    @property
    def corruption_detection_threshold(self) -> float:
        return self.get_float("CORRUPTION_DETECTION_THRESHOLD", 0.80)
    
    @property
    def max_consecutive_corrupted_chunks(self) -> int:
        return self.get_int("MAX_CONSECUTIVE_CORRUPTED_CHUNKS", 50)
    
    @property
    def enable_fast_corruption_recovery(self) -> bool:
        return self.get_bool("ENABLE_FAST_CORRUPTION_RECOVERY", True)
    
    # Stream Reliability Configuration
    @property
    def max_stream_silence_seconds(self) -> int:
        return self.get_int("MAX_STREAM_SILENCE_SECONDS", 30)
    
    @property
    def tiktok_pause_detection_seconds(self) -> int:
        return self.get_int("TIKTOK_PAUSE_DETECTION_SECONDS", 15)
    
    @property
    def max_pause_duration_seconds(self) -> int:
        return self.get_int("MAX_PAUSE_DURATION_SECONDS", 120)
    
    @property
    def enable_pause_detection(self) -> bool:
        return self.get_bool("ENABLE_PAUSE_DETECTION", True)
    
    @property
    def max_reconnection_attempts(self) -> int:
        return self.get_int("MAX_RECONNECTION_ATTEMPTS", 8)
    
    @property
    def reconnection_retry_interval(self) -> int:
        return self.get_int("RECONNECTION_RETRY_INTERVAL", 10)
    
    @property
    def enable_aggressive_reconnection(self) -> bool:
        return self.get_bool("ENABLE_AGGRESSIVE_RECONNECTION", True)
    
    @property
    def normal_gap_timeout_seconds(self) -> int:
        return self.get_int("NORMAL_GAP_TIMEOUT_SECONDS", 20)
    
    @property
    def preventive_reconnect_interval_minutes(self) -> int:
        return self.get_int("PREVENTIVE_RECONNECT_INTERVAL_MINUTES", 10)
    
    @property
    def enable_advanced_ffmpeg_correction(self) -> bool:
        return self.get_bool("ENABLE_ADVANCED_FFMPEG_CORRECTION", False)
    
    # Segment-based Recording Configuration
    @property
    def enable_segment_recording(self) -> bool:
        return self.get_bool("ENABLE_SEGMENT_RECORDING", False)
    
    @property
    def max_segment_duration_minutes(self) -> int:
        return self.get_int("MAX_SEGMENT_DURATION_MINUTES", 5)
    
    @property
    def segment_transition_delay_seconds(self) -> int:
        return self.get_int("SEGMENT_TRANSITION_DELAY_SECONDS", 6)
    
    @property
    def live_restart_merge_window_minutes(self) -> int:
        return self.get_int("LIVE_RESTART_MERGE_WINDOW_MINUTES", 15)
    
    @property
    def enable_automatic_segment_concatenation(self) -> bool:
        return self.get_bool("ENABLE_AUTOMATIC_SEGMENT_CONCATENATION", True)
    
    @property
    def fallback_to_classic_on_failure(self) -> bool:
        return self.get_bool("FALLBACK_TO_CLASSIC_ON_FAILURE", True)
    
    # Video Collage Configuration
    @property
    def enable_video_collage(self) -> bool:
        return self.get_bool("ENABLE_VIDEO_COLLAGE", True)
    
    @property
    def collage_screenshot_count(self) -> int:
        return self.get_int("COLLAGE_SCREENSHOT_COUNT", 14)
    
    @property
    def collage_width(self) -> int:
        return self.get_int("COLLAGE_WIDTH", 1200)
    
    @property
    def collage_height(self) -> int:
        return self.get_int("COLLAGE_HEIGHT", 800)
    
    @property
    def collage_grid_rows(self) -> int:
        return self.get_int("COLLAGE_GRID_ROWS", 4)
    
    @property
    def collage_grid_cols(self) -> int:
        return self.get_int("COLLAGE_GRID_COLS", 4)
    
    @property
    def collage_watermark_text(self) -> str:
        return self.get_str("COLLAGE_WATERMARK_TEXT", "Recorded with @RecLiveBot")
    
    # Database Configuration
    @property
    def db_connection_timeout(self) -> int:
        return self.get_int("DB_CONNECTION_TIMEOUT_SECONDS", 30)
    
    @property
    def db_pool_size(self) -> int:
        return self.get_int("DB_POOL_SIZE", 10)
    
    @property
    def db_max_overflow(self) -> int:
        return self.get_int("DB_MAX_OVERFLOW", 20)
    
    # Error Handling
    @property
    def enable_fallback_api(self) -> bool:
        return self.get_bool("ENABLE_FALLBACK_API", True)
    
    @property
    def fallback_on_protocol_error(self) -> bool:
        return self.get_bool("FALLBACK_ON_PROTOCOL_ERROR", False)
    
    @property
    def skip_cache_on_connection_error(self) -> bool:
        return self.get_bool("SKIP_CACHE_ON_CONNECTION_ERROR", True)
    
    # Deterministic Configuration for Cross-Machine Consistency
    @property
    def deterministic_recording(self) -> bool:
        return self.get_bool("DETERMINISTIC_RECORDING", False)
    
    @property
    def fixed_user_agent_index(self) -> int:
        return self.get_int("FIXED_USER_AGENT_INDEX", 0)
    
    # Work Queue System Configuration
    @property
    def job_data_expiry_seconds(self) -> int:
        return self.get_int("JOB_DATA_EXPIRY_SECONDS", 86400)  # 24 hours
    
    @property
    def user_jobs_expiry_seconds(self) -> int:
        return self.get_int("USER_JOBS_EXPIRY_SECONDS", 3600)  # 1 hour
    
    @property
    def worker_info_expiry_seconds(self) -> int:
        return self.get_int("WORKER_INFO_EXPIRY_SECONDS", 300)  # 5 minutes
    
    @property
    def recording_status_expiry_seconds(self) -> int:
        return self.get_int("RECORDING_STATUS_EXPIRY_SECONDS", 1800)  # 30 minutes
    
    @property
    def worker_heartbeat_interval_seconds(self) -> int:
        return self.get_int("WORKER_HEARTBEAT_INTERVAL_SECONDS", 30)

    @property
    def worker_heartbeat_timeout_seconds(self) -> int:
        return self.get_int("WORKER_HEARTBEAT_TIMEOUT_SECONDS", 90)

    @property
    def worker_max_concurrent_jobs(self) -> int:
        return self.get_int("WORKER_MAX_CONCURRENT_JOBS", 5)
    
    @property
    def job_cleanup_interval_hours(self) -> int:
        return self.get_int("JOB_CLEANUP_INTERVAL_HOURS", 6)
    
    @property
    def enable_distributed_recording(self) -> bool:
        return self.get_bool("ENABLE_DISTRIBUTED_RECORDING", False)
    
    @property
    def disable_random_delays(self) -> bool:
        return self.get_bool("DISABLE_RANDOM_DELAYS", False)
    
    @property
    def consistent_retry_intervals(self) -> bool:
        return self.get_bool("CONSISTENT_RETRY_INTERVALS", False)
    
    @property
    def standardize_timeouts(self) -> bool:
        return self.get_bool("STANDARDIZE_TIMEOUTS", False)
    
    # FFmpeg Direct Recording Configuration
    @property
    def enable_ffmpeg_direct_recording(self) -> bool:
        return self.get_bool("ENABLE_FFMPEG_DIRECT_RECORDING", True)
    
    @property
    def ffmpeg_reconnect_attempts(self) -> int:
        return self.get_int("FFMPEG_RECONNECT_ATTEMPTS", 30)
    
    @property
    def ffmpeg_reconnect_delay_max(self) -> int:
        return self.get_int("FFMPEG_RECONNECT_DELAY_MAX", 10)
    
    @property
    def ffmpeg_user_agent(self) -> str:
        return self.get_str("FFMPEG_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    @property
    def ffmpeg_timeout(self) -> int:
        return self.get_int("FFMPEG_TIMEOUT_SECONDS", 20)
    
    @property
    def ffmpeg_loglevel(self) -> str:
        return self.get_str("FFMPEG_LOGLEVEL", "info")
    
    @property
    def ffmpeg_error_detection(self) -> str:
        return self.get_str("FFMPEG_ERROR_DETECTION", "ignore_err")
    
    @property
    def ffmpeg_flags(self) -> str:
        return self.get_str("FFMPEG_FLAGS", "+discardcorrupt+igndts+genpts")
    
    # Continuous Recording Configuration
    @property
    def enable_continuous_recording(self) -> bool:
        return self.get_bool("ENABLE_CONTINUOUS_RECORDING", True)
    
    @property
    def post_recording_validation_minutes(self) -> int:
        return self.get_int("POST_RECORDING_VALIDATION_MINUTES", 2)
    
    @property
    def max_recording_parts(self) -> int:
        return self.get_int("MAX_RECORDING_PARTS", 150)
    
    @property
    def auto_restart_delay_seconds(self) -> int:
        return self.get_int("AUTO_RESTART_DELAY_SECONDS", 5)
    
    @property
    def enable_smart_concatenation(self) -> bool:
        return self.get_bool("ENABLE_SMART_CONCATENATION", True)
    
    # Frame Validation Configuration
    @property
    def enable_frame_validation(self) -> bool:
        return self.get_bool("ENABLE_FRAME_VALIDATION", True)
    
    @property
    def min_frames_threshold(self) -> int:
        return self.get_int("MIN_FRAMES_THRESHOLD", 5)
    
    @property
    def auto_cleanup_pause_videos(self) -> bool:
        return self.get_bool("AUTO_CLEANUP_PAUSE_VIDEOS", True)
    
    # FFprobe Configuration for Performance Optimization
    @property
    def enable_ffprobe_verification(self) -> bool:
        return self.get_bool("ENABLE_FFPROBE_VERIFICATION", False)
    
    @property
    def ffprobe_verification_mode(self) -> str:
        return self.get_str("FFPROBE_VERIFICATION_MODE", "disabled")  # disabled, basic, full
    
    # FFmpeg Logging Configuration
    @property
    def enable_ffmpeg_logs(self) -> bool:
        return self.get_bool("ENABLE_FFMPEG_LOGS", False)
    
    # FFmpeg Memory Guardian Configuration
    @property
    def enable_ffmpeg_memory_guardian(self) -> bool:
        return self.get_bool("ENABLE_FFMPEG_MEMORY_GUARDIAN", True)
    
    @property
    def ffmpeg_memory_threshold_percent(self) -> float:
        return self.get_float("FFMPEG_MEMORY_THRESHOLD_PERCENT", 6.0)
    
    @property
    def memory_check_interval_seconds(self) -> int:
        return self.get_int("MEMORY_CHECK_INTERVAL_SECONDS", 10)
    
    # AVCC Error Postprocessing Configuration
    @property
    def enable_avcc_postprocessing(self) -> bool:
        return self.get_bool("ENABLE_AVCC_POSTPROCESSING", True)
    
    @property
    def avcc_trim_seconds(self) -> int:
        return self.get_int("AVCC_TRIM_SECONDS", 2)

    # Post-processing Queue Configuration
    @property
    def enable_postprocessing_queue(self) -> bool:
        return self.get_bool("ENABLE_POSTPROCESSING_QUEUE", True)
    
    @property
    def postprocessing_workers(self) -> int:
        return self.get_int("POSTPROCESSING_WORKERS", 2)
    
    @property
    def min_segments_for_queue(self) -> int:
        return self.get_int("MIN_SEGMENTS_FOR_QUEUE", 3)
    
    @property
    def max_queue_size(self) -> int:
        return self.get_int("MAX_QUEUE_SIZE", 60)
    
    @property
    def postprocessing_timeout_minutes(self) -> int:
        return self.get_int("POSTPROCESSING_TIMEOUT_MINUTES", 15)
    
    @property
    def queue_corruption_threshold_mb(self) -> int:
        return self.get_int("QUEUE_CORRUPTION_THRESHOLD_MB", 100)

    # FFmpeg Grace Period Configuration - SOLO lo solicitado
    @property
    def ffmpeg_grace_timeout_seconds(self) -> int:
        return self.get_int("FFMPEG_GRACE_TIMEOUT_SECONDS", 30)

    # Corrupted Segment Detection Configuration
    @property
    def corrupted_segment_ffprobe_timeout_seconds(self) -> int:
        return self.get_int("CORRUPTED_SEGMENT_FFPROBE_TIMEOUT_SECONDS", 10)
    
    @property
    def corrupted_segment_max_file_size_mb(self) -> int:
        return self.get_int("CORRUPTED_SEGMENT_MAX_FILE_SIZE_MB", 60)

    # Frame Repetition Detection Configuration
    @property
    def frame_repetition_threshold(self) -> int:
        return self.get_int("FRAME_REPETITION_THRESHOLD", 1)
    
    @property
    def enable_timestamp_frame_detection(self) -> bool:
        return self.get_bool("ENABLE_TIMESTAMP_FRAME_DETECTION", True)

    # MKV to MP4 Conversion Configuration
    @property
    def mkv_to_mp4_timeout_seconds(self) -> int:
        return self.get_int("MKV_TO_MP4_TIMEOUT_SECONDS", 600)
    
    @property
    def mkv_to_mp4_timeout_per_gb_seconds(self) -> int:
        return self.get_int("MKV_TO_MP4_TIMEOUT_PER_GB_SECONDS", 60)

    # Real-time Video Sampling Configuration
    @property
    def enable_realtime_sampling(self) -> bool:
        return self.get_bool("ENABLE_REALTIME_SAMPLING", True)
    
    @property
    def sampling_interval_seconds(self) -> int:
        return self.get_int("SAMPLING_INTERVAL_SECONDS", 30)
    
    @property
    def sample_duration_seconds(self) -> int:
        return self.get_int("SAMPLE_DURATION_SECONDS", 10)
    
    @property
    def sample_corruption_threshold_percent(self) -> float:
        return self.get_float("SAMPLE_CORRUPTION_THRESHOLD_PERCENT", 30.0)
    
    @property
    def sample_analysis_timeout_seconds(self) -> int:
        return self.get_int("SAMPLE_ANALYSIS_TIMEOUT_SECONDS", 15)
    
    @property
    def min_recording_duration_before_sampling_seconds(self) -> int:
        return self.get_int("MIN_RECORDING_DURATION_BEFORE_SAMPLING_SECONDS", 20)

    # Initial Stream Stabilization Configuration
    @property
    def enable_initial_skip(self) -> bool:
        return self.get_bool("ENABLE_INITIAL_SKIP", False)
    
    @property
    def initial_skip_seconds(self) -> int:
        return self.get_int("INITIAL_SKIP_SECONDS", 5)

    # URL Expiration Recovery Configuration
    @property
    def enable_url_recovery(self) -> bool:
        return self.get_bool("ENABLE_URL_RECOVERY", True)
    
    @property
    def url_recovery_max_attempts(self) -> int:
        return self.get_int("URL_RECOVERY_MAX_ATTEMPTS", 3)
    
    @property
    def url_recovery_delay_seconds(self) -> int:
        return self.get_int("URL_RECOVERY_DELAY_SECONDS", 10)

    # Message Forwarding Configuration
    @property
    def enable_message_forwarding(self) -> bool:
        return self.get_bool("ENABLE_MESSAGE_FORWARDING", True)
    
    @property
    def forwarding_retry_attempts(self) -> int:
        return self.get_int("FORWARDING_RETRY_ATTEMPTS", 3)
    
    @property
    def forwarding_retry_delay_seconds(self) -> int:
        return self.get_int("FORWARDING_RETRY_DELAY_SECONDS", 2)

    # Upload Queue Persistence Configuration
    @property
    def enable_upload_queue_persistence(self) -> bool:
        return self.get_bool("ENABLE_UPLOAD_QUEUE_PERSISTENCE", True)
    
    @property
    def upload_queue_state_file(self) -> str:
        return self.get_str("UPLOAD_QUEUE_STATE_FILE", "upload_queue_state.json")
    
    @property
    def upload_recovery_scan_enabled(self) -> bool:
        return self.get_bool("ENABLE_UPLOAD_RECOVERY_SCAN", True)
    
    @property
    def upload_recovery_interval_minutes(self) -> int:
        return self.get_int("UPLOAD_RECOVERY_INTERVAL_MINUTES", 30)

    # Resource Monitoring Configuration
    @property
    def enable_resource_monitoring(self) -> bool:
        return self.get_bool("ENABLE_RESOURCE_MONITORING", True)
    
    @property
    def max_file_descriptors_warning(self) -> int:
        return self.get_int("MAX_FILE_DESCRIPTORS_WARNING", 3000)
    
    @property
    def max_file_descriptors_critical(self) -> int:
        return self.get_int("MAX_FILE_DESCRIPTORS_CRITICAL", 5000)
    
    @property
    def system_file_descriptors_limit(self) -> int:
        return self.get_int("SYSTEM_FILE_DESCRIPTORS_LIMIT", 0)  # 0 = auto-detect

    # Redis Configuration for Recording Subscriptions
    @property
    def redis_url(self) -> str:
        return self.get_str("REDIS_URL", "redis://localhost:6379")
    
    @property
    def redis_max_connections(self) -> int:
        return self.get_int("REDIS_MAX_CONNECTIONS", 10)
    
    @property
    def recording_subscription_ttl_days(self) -> int:
        return self.get_int("RECORDING_SUBSCRIPTION_TTL_DAYS", 7)
    
    @property
    def enable_recording_persistence(self) -> bool:
        return self.get_bool("ENABLE_RECORDING_PERSISTENCE", True)
    
    @property
    def recording_timeout_hours(self) -> int:
        return self.get_int("RECORDING_TIMEOUT_HOURS", 3)
    
    @property
    def forwarding_max_retry_attempts(self) -> int:
        return self.get_int("FORWARDING_MAX_RETRY_ATTEMPTS", 3)
    
    @property
    def redis_cleanup_interval_minutes(self) -> int:
        return self.get_int("REDIS_CLEANUP_INTERVAL_MINUTES", 60)

    # Gap Detection Configuration
    @property
    def enable_gap_detection(self) -> bool:
        return self.get_bool("ENABLE_GAP_DETECTION", True)
    
    @property
    def gap_detection_threshold_seconds(self) -> int:
        return self.get_int("GAP_DETECTION_THRESHOLD_SECONDS", 15)
    
    @property
    def gap_trimming_buffer_seconds(self) -> int:
        return self.get_int("GAP_TRIMMING_BUFFER_SECONDS", 2)

    # Orphaned Redis Cleanup Configuration
    @property
    def enable_redis_orphan_cleanup(self) -> bool:
        return self.get_bool("ENABLE_REDIS_ORPHAN_CLEANUP", True)
    
    @property
    def redis_orphan_cleanup_interval_hours(self) -> int:
        return self.get_int("REDIS_ORPHAN_CLEANUP_INTERVAL_HOURS", 1)
    
    @property
    def redis_orphan_timeout_hours(self) -> int:
        return self.get_int("REDIS_ORPHAN_TIMEOUT_HOURS", 3)

    # Zombie Recording Detection Configuration
    @property
    def suspicious_recording_threshold_hours(self) -> int:
        return self.get_int("SUSPICIOUS_RECORDING_THRESHOLD_HOURS", 3)
    
    @property
    def zombie_recording_threshold_hours(self) -> int:
        return self.get_int("ZOMBIE_RECORDING_THRESHOLD_HOURS", 6)

    # Restart Retry Logic After Corruption Configuration
    @property
    def max_restart_attempts_after_corruption(self) -> int:
        return self.get_int("MAX_RESTART_ATTEMPTS_AFTER_CORRUPTION", 3)
    
    @property
    def restart_retry_delay_seconds(self) -> int:
        return self.get_int("RESTART_RETRY_DELAY_SECONDS", 15)
    
    @property
    def force_url_refresh_delay_seconds(self) -> int:
        return self.get_int("FORCE_URL_REFRESH_DELAY_SECONDS", 5)

    # =====================================================
    # Telegram Caption Configuration
    # =====================================================

    @property
    def caption_show_fragment_info(self) -> bool:
        return self.get_bool("CAPTION_SHOW_FRAGMENT_INFO", True)

    @property
    def caption_show_date(self) -> bool:
        return self.get_bool("CAPTION_SHOW_DATE", False)

    @property
    def caption_show_video_info(self) -> bool:
        return self.get_bool("CAPTION_SHOW_VIDEO_INFO", False)

    @property
    def caption_show_watermark(self) -> bool:
        return self.get_bool("CAPTION_SHOW_WATERMARK", True)

    @property
    def caption_watermark_text(self) -> str:
        return self.get_str("CAPTION_WATERMARK_TEXT", "🛑<b>Live recorded with @RecLiveBot</b>")


# Global configuration instance
config = EnvConfig()