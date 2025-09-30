import logging
import os
from datetime import datetime

class LoggerManager:

    _instance = None  # Singleton instance

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LoggerManager, cls).__new__(cls)
            cls._instance.logger = None
            cls._instance.setup_logger()
        return cls._instance

    def setup_logger(self):
        if self.logger is None:
            # Configurar el logger raíz para evitar conflictos
            root_logger = logging.getLogger()
            root_logger.handlers.clear()
            root_logger.setLevel(logging.INFO)
            
            # Configurar loggers específicos para evitar duplicación
            pyrogram_logger = logging.getLogger('pyrogram')
            pyrogram_logger.handlers.clear()
            pyrogram_logger.propagate = False
            
            # Configurar nuestro logger principal
            self.logger = logging.getLogger('logger')
            self.logger.setLevel(logging.INFO)
            
            # Limpiar handlers existentes para evitar duplicación
            self.logger.handlers.clear()

            # Crear directorio de logs si no existe
            log_dir = 'logs'
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            # Generar nombre de archivo con fecha
            current_date = datetime.now().strftime('%Y-%m-%d')
            log_file = os.path.join(log_dir, f'bot_log_{current_date}.txt')

            # File handler (para todos los niveles)
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            file_format = '%(asctime)s - %(levelname)s - %(message)s'
            file_datefmt = '%Y-%m-%d %H:%M:%S'
            file_formatter = logging.Formatter(file_format, file_datefmt)
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)

            # Console handler (para todos los niveles)
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # Formato personalizado para consola
            def custom_format(record):
                if record.levelno >= logging.ERROR:
                    return f"[!] {record.asctime} - {record.getMessage()}"
                else:
                    return f"[*] {record.asctime} - {record.getMessage()}"
            
            class CustomFormatter(logging.Formatter):
                def format(self, record):
                    return custom_format(record)
            
            console_formatter = CustomFormatter(datefmt='%Y-%m-%d %H:%M:%S')
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)

            # Log inicial
            self.logger.info(f"Logger initialized. Log file: {log_file}")

    def info(self, message):
        """
        Log an INFO-level message.
        """
        self.logger.info(message)

    def error(self, message):
        """
        Log an ERROR-level message.
        """
        self.logger.error(message)

    def warning(self, message):
        """
        Log a WARNING-level message.
        """
        self.logger.warning(message)

logger = LoggerManager().logger
