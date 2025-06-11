"""
Logging Manager for Crypto Data Importer
Handles logging configuration and management
"""

import logging
import logging.handlers
from typing import Optional
from configuration_manager import ConfigurationManager


class LoggingManager:
    """Manages logging configuration and setup"""
    
    def __init__(self, config: ConfigurationManager):
        self.config = config
        self._loggers = {}
        self.setup_logging()
    
    def setup_logging(self) -> bool:
        """Setup logging based on configuration"""
        try:
            log_level = self.config.get('LOGGING', 'log_level', 'INFO')
            log_file = self.config.get('LOGGING', 'log_file')
            
            # Convert string log level to logging constant
            numeric_level = getattr(logging, log_level.upper(), logging.INFO)
            
            # Configure root logger
            root_logger = logging.getLogger()
            root_logger.setLevel(numeric_level)
            
            # Clear existing handlers
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
            
            # Setup console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(numeric_level)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)
            
            # Setup file handler if specified
            if log_file:
                self._setup_file_handler(root_logger, log_file, numeric_level, formatter)
            
            return True
            
        except Exception as e:
            print(f"Failed to setup logging: {e}")
            return False
    
    def _setup_file_handler(self, logger, log_file: str, level: int, formatter):
        """Setup rotating file handler"""
        try:
            max_size = self.config.getint('LOGGING', 'max_log_size_mb') * 1024 * 1024
            backup_count = self.config.getint('LOGGING', 'backup_count')
            
            file_handler = logging.handlers.RotatingFileHandler(
                log_file, maxBytes=max_size, backupCount=backup_count
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            logging.info(f"Logging to file: {log_file}")
            
        except Exception as e:
            logging.warning(f"Could not setup file logging: {e}")
    
    def get_logger(self, name: str) -> logging.Logger:
        """Get or create a logger with the specified name"""
        if name not in self._loggers:
            self._loggers[name] = logging.getLogger(name)
        return self._loggers[name]
    
    def rotate_logs(self) -> bool:
        """Manually rotate log files"""
        try:
            root_logger = logging.getLogger()
            for handler in root_logger.handlers:
                if isinstance(handler, logging.handlers.RotatingFileHandler):
                    handler.doRollover()
            return True
        except Exception as e:
            logging.error(f"Failed to rotate logs: {e}")
            return False
    
    def set_level(self, level: str):
        """Change logging level dynamically"""
        try:
            numeric_level = getattr(logging, level.upper(), logging.INFO)
            root_logger = logging.getLogger()
            root_logger.setLevel(numeric_level)
            
            for handler in root_logger.handlers:
                handler.setLevel(numeric_level)
                
            logging.info(f"Log level changed to: {level}")
            
        except Exception as e:
            logging.error(f"Failed to set log level: {e}")
