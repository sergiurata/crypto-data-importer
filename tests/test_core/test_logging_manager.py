"""
Test cases for LoggingManager
"""

import unittest
import tempfile
import os
import logging
import logging.handlers
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

from logging_manager import LoggingManager
from configuration_manager import ConfigurationManager


class TestLoggingManager(unittest.TestCase):
    """Test cases for LoggingManager class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create mock configuration
        self.mock_config = Mock(spec=ConfigurationManager)
        self.mock_config.get.return_value = ""
        self.mock_config.getint.return_value = 5
        
        # Create temporary directory for log files
        self.temp_dir = tempfile.mkdtemp()
        self.test_log_file = os.path.join(self.temp_dir, "test.log")
        
        # Clear any existing handlers
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Reset logging level
        root_logger.setLevel(logging.WARNING)
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Remove log file if exists
        if os.path.exists(self.test_log_file):
            os.remove(self.test_log_file)
        os.rmdir(self.temp_dir)
        
        # Clear handlers again
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
    
    def test_init_creates_logging_manager(self):
        """Test that LoggingManager initializes correctly"""
        with patch.object(LoggingManager, 'setup_logging') as mock_setup:
            logging_manager = LoggingManager(self.mock_config)
            
            self.assertEqual(logging_manager.config, self.mock_config)
            self.assertEqual(logging_manager._loggers, {})
            mock_setup.assert_called_once()
    
    def test_setup_logging_console_only(self):
        """Test setting up console logging only"""
        self.mock_config.get.side_effect = lambda section, key, fallback="": {
            ('LOGGING', 'log_level'): 'INFO',
            ('LOGGING', 'log_file'): ''  # No file logging
        }.get((section, key), fallback)
        
        logging_manager = LoggingManager(self.mock_config)
        
        # Check that root logger is configured
        root_logger = logging.getLogger()
        self.assertEqual(root_logger.level, logging.INFO)
        
        # Should have at least one handler (console)
        self.assertGreater(len(root_logger.handlers), 0)
        
        # First handler should be StreamHandler (console)
        self.assertIsInstance(root_logger.handlers[0], logging.StreamHandler)
    
    def test_setup_logging_with_file(self):
        """Test setting up logging with file output"""
        self.mock_config.get.side_effect = lambda section, key, fallback="": {
            ('LOGGING', 'log_level'): 'DEBUG',
            ('LOGGING', 'log_file'): self.test_log_file
        }.get((section, key), fallback)
        
        self.mock_config.getint.side_effect = lambda section, key, fallback=0: {
            ('LOGGING', 'max_log_size_mb'): 10,
            ('LOGGING', 'backup_count'): 3
        }.get((section, key), fallback)
        
        logging_manager = LoggingManager(self.mock_config)
        
        # Check that root logger is configured
        root_logger = logging.getLogger()
        self.assertEqual(root_logger.level, logging.DEBUG)
        
        # Should have at least 2 handlers (console + file)
        self.assertGreaterEqual(len(root_logger.handlers), 2)
        
        # Check for RotatingFileHandler
        file_handlers = [h for h in root_logger.handlers 
                        if isinstance(h, logging.handlers.RotatingFileHandler)]
        self.assertEqual(len(file_handlers), 1)
        
        # Verify file handler properties
        file_handler = file_handlers[0]
        self.assertEqual(file_handler.maxBytes, 10 * 1024 * 1024)  # 10MB
        self.assertEqual(file_handler.backupCount, 3)
    
    def test_setup_logging_invalid_level(self):
        """Test setup with invalid log level defaults to INFO"""
        self.mock_config.get.side_effect = lambda section, key, fallback="": {
            ('LOGGING', 'log_level'): 'INVALID_LEVEL',
            ('LOGGING', 'log_file'): ''
        }.get((section, key), fallback)
        
        logging_manager = LoggingManager(self.mock_config)
        
        # Should default to INFO level
        root_logger = logging.getLogger()
        self.assertEqual(root_logger.level, logging.INFO)
    
    def test_setup_logging_file_permission_error(self):
        """Test handling of file permission errors"""
        # Create read-only directory to trigger permission error
        readonly_dir = os.path.join(self.temp_dir, "readonly")
        os.makedirs(readonly_dir)
        os.chmod(readonly_dir, 0o444)  # Read-only
        
        readonly_log = os.path.join(readonly_dir, "test.log")
        
        self.mock_config.get.side_effect = lambda section, key, fallback="": {
            ('LOGGING', 'log_level'): 'INFO',
            ('LOGGING', 'log_file'): readonly_log
        }.get((section, key), fallback)
        
        # Should not raise exception, just log warning
        with patch('logging_manager.logging') as mock_logging:
            logging_manager = LoggingManager(self.mock_config)
            
            # Should have console handler only
            root_logger = logging.getLogger()
            console_handlers = [h for h in root_logger.handlers 
                              if isinstance(h, logging.StreamHandler)]
            self.assertGreater(len(console_handlers), 0)
        
        # Cleanup
        os.chmod(readonly_dir, 0o755)
        os.rmdir(readonly_dir)
    
    def test_get_logger_new(self):
        """Test getting a new logger"""
        logging_manager = LoggingManager(self.mock_config)
        
        logger = logging_manager.get_logger("test_module")
        
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.name, "test_module")
        self.assertIn("test_module", logging_manager._loggers)
    
    def test_get_logger_existing(self):
        """Test getting an existing logger returns same instance"""
        logging_manager = LoggingManager(self.mock_config)
        
        logger1 = logging_manager.get_logger("test_module")
        logger2 = logging_manager.get_logger("test_module")
        
        self.assertIs(logger1, logger2)
        self.assertEqual(len(logging_manager._loggers), 1)
    
    def test_rotate_logs_success(self):
        """Test successful log rotation"""
        # Setup file logging
        self.mock_config.get.side_effect = lambda section, key, fallback="": {
            ('LOGGING', 'log_level'): 'INFO',
            ('LOGGING', 'log_file'): self.test_log_file
        }.get((section, key), fallback)
        
        logging_manager = LoggingManager(self.mock_config)
        
        # Mock the rotating file handler
        with patch('logging.handlers.RotatingFileHandler') as mock_handler_class:
            mock_handler = MagicMock()
            mock_handler_class.return_value = mock_handler
            
            # Create new logging manager to get mocked handler
            logging_manager = LoggingManager(self.mock_config)
            
            # Add the mock handler to root logger
            root_logger = logging.getLogger()
            root_logger.addHandler(mock_handler)
            
            result = logging_manager.rotate_logs()
            
            self.assertTrue(result)
            mock_handler.doRollover.assert_called_once()
    
    def test_rotate_logs_no_rotating_handlers(self):
        """Test log rotation when no rotating handlers exist"""
        # Setup console-only logging
        self.mock_config.get.side_effect = lambda section, key, fallback="": {
            ('LOGGING', 'log_level'): 'INFO',
            ('LOGGING', 'log_file'): ''
        }.get((section, key), fallback)
        
        logging_manager = LoggingManager(self.mock_config)
        
        # Should return True even with no rotating handlers
        result = logging_manager.rotate_logs()
        self.assertTrue(result)
    
    def test_rotate_logs_exception(self):
        """Test log rotation handles exceptions"""
        logging_manager = LoggingManager(self.mock_config)
        
        # Mock handler that raises exception
        mock_handler = MagicMock()
        mock_handler.doRollover.side_effect = Exception("Rotation failed")
        
        root_logger = logging.getLogger()
        root_logger.addHandler(mock_handler)
        
        result = logging_manager.rotate_logs()
        
        self.assertFalse(result)
    
    def test_set_level_valid(self):
        """Test setting valid log level"""
        logging_manager = LoggingManager(self.mock_config)
        
        logging_manager.set_level("DEBUG")
        
        root_logger = logging.getLogger()
        self.assertEqual(root_logger.level, logging.DEBUG)
        
        # All handlers should also have the new level
        for handler in root_logger.handlers:
            self.assertEqual(handler.level, logging.DEBUG)
    
    def test_set_level_invalid(self):
        """Test setting invalid log level"""
        logging_manager = LoggingManager(self.mock_config)
        
        # Should not raise exception, defaults to INFO
        logging_manager.set_level("INVALID")
        
        root_logger = logging.getLogger()
        self.assertEqual(root_logger.level, logging.INFO)
    
    def test_set_level_case_insensitive(self):
        """Test that log level setting is case insensitive"""
        logging_manager = LoggingManager(self.mock_config)
        
        logging_manager.set_level("warning")
        
        root_logger = logging.getLogger()
        self.assertEqual(root_logger.level, logging.WARNING)
    
    def test_formatter_configuration(self):
        """Test that formatters are properly configured"""
        self.mock_config.get.side_effect = lambda section, key, fallback="": {
            ('LOGGING', 'log_level'): 'INFO',
            ('LOGGING', 'log_file'): self.test_log_file
        }.get((section, key), fallback)
        
        logging_manager = LoggingManager(self.mock_config)
        
        root_logger = logging.getLogger()
        
        # All handlers should have formatters
        for handler in root_logger.handlers:
            self.assertIsNotNone(handler.formatter)
            
            # Check formatter format string contains expected elements
            format_string = handler.formatter._fmt
            self.assertIn('%(asctime)s', format_string)
            self.assertIn('%(levelname)s', format_string)
            self.assertIn('%(message)s', format_string)
    
    def test_logging_output_console(self):
        """Test actual logging output to console"""
        # Capture console output
        console_capture = StringIO()
        
        self.mock_config.get.side_effect = lambda section, key, fallback="": {
            ('LOGGING', 'log_level'): 'INFO',
            ('LOGGING', 'log_file'): ''
        }.get((section, key), fallback)
        
        with patch('sys.stderr', console_capture):
            logging_manager = LoggingManager(self.mock_config)
            test_logger = logging_manager.get_logger("test")
            
            test_logger.info("Test message")
            test_logger.error("Test error")
        
        # Check output contains our messages
        output = console_capture.getvalue()
        self.assertIn("Test message", output)
        self.assertIn("Test error", output)
        self.assertIn("INFO", output)
        self.assertIn("ERROR", output)
    
    def test_logging_output_file(self):
        """Test actual logging output to file"""
        self.mock_config.get.side_effect = lambda section, key, fallback="": {
            ('LOGGING', 'log_level'): 'DEBUG',
            ('LOGGING', 'log_file'): self.test_log_file
        }.get((section, key), fallback)
        
        self.mock_config.getint.side_effect = lambda section, key, fallback=0: {
            ('LOGGING', 'max_log_size_mb'): 1,
            ('LOGGING', 'backup_count'): 1
        }.get((section, key), fallback)
        
        logging_manager = LoggingManager(self.mock_config)
        test_logger = logging_manager.get_logger("test")
        
        test_logger.debug("Debug message")
        test_logger.info("Info message")
        test_logger.warning("Warning message")
        
        # Force flush handlers
        for handler in logging.getLogger().handlers:
            handler.flush()
        
        # Check file contains our messages
        self.assertTrue(os.path.exists(self.test_log_file))
        
        with open(self.test_log_file, 'r') as f:
            content = f.read()
            self.assertIn("Debug message", content)
            self.assertIn("Info message", content)
            self.assertIn("Warning message", content)
    
    def test_multiple_loggers(self):
        """Test managing multiple named loggers"""
        logging_manager = LoggingManager(self.mock_config)
        
        logger1 = logging_manager.get_logger("module1")
        logger2 = logging_manager.get_logger("module2")
        logger3 = logging_manager.get_logger("module3")
        
        self.assertEqual(len(logging_manager._loggers), 3)
        self.assertIn("module1", logging_manager._loggers)
        self.assertIn("module2", logging_manager._loggers)
        self.assertIn("module3", logging_manager._loggers)
        
        # Each should be a different logger
        self.assertIsNot(logger1, logger2)
        self.assertIsNot(logger2, logger3)
        
        # But getting same name should return same instance
        logger1_again = logging_manager.get_logger("module1")
        self.assertIs(logger1, logger1_again)
    
    def test_log_level_hierarchy(self):
        """Test that log levels work correctly"""
        self.mock_config.get.side_effect = lambda section, key, fallback="": {
            ('LOGGING', 'log_level'): 'WARNING',
            ('LOGGING', 'log_file'): ''
        }.get((section, key), fallback)
        
        console_capture = StringIO()
        
        with patch('sys.stderr', console_capture):
            logging_manager = LoggingManager(self.mock_config)
            test_logger = logging_manager.get_logger("test")
            
            # These should not appear (below WARNING level)
            test_logger.debug("Debug message")
            test_logger.info("Info message")
            
            # These should appear (WARNING level and above)
            test_logger.warning("Warning message")
            test_logger.error("Error message")
            test_logger.critical("Critical message")
        
        output = console_capture.getvalue()
        
        # Low level messages should not appear
        self.assertNotIn("Debug message", output)
        self.assertNotIn("Info message", output)
        
        # High level messages should appear
        self.assertIn("Warning message", output)
        self.assertIn("Error message", output)
        self.assertIn("Critical message", output)


class TestLoggingManagerIntegration(unittest.TestCase):
    """Integration tests for LoggingManager"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.temp_dir, "test_config.ini")
        
        # Clear existing handlers
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
    
    def tearDown(self):
        """Clean up test fixtures"""
        if os.path.exists(self.config_path):
            os.remove(self.config_path)
        
        # Clean up any log files
        for file in os.listdir(self.temp_dir):
            file_path = os.path.join(self.temp_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        
        os.rmdir(self.temp_dir)
        
        # Clear handlers
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
    
    def test_integration_with_configuration_manager(self):
        """Test integration with actual ConfigurationManager"""
        from configuration_manager import ConfigurationManager
        
        # Create test configuration file
        config_content = """
[LOGGING]
log_level = DEBUG
log_file = {}/integration_test.log
max_log_size_mb = 5
backup_count = 2
""".format(self.temp_dir.replace('\\', '/'))
        
        with open(self.config_path, 'w') as f:
            f.write(config_content)
        
        # Create real configuration manager
        config = ConfigurationManager(self.config_path)
        
        # Create logging manager
        logging_manager = LoggingManager(config)
        
        # Test logging functionality
        test_logger = logging_manager.get_logger("integration_test")
        test_logger.info("Integration test message")
        test_logger.error("Integration test error")
        
        # Verify file was created and contains messages
        log_file_path = os.path.join(self.temp_dir, "integration_test.log")
        self.assertTrue(os.path.exists(log_file_path))
        
        # Force flush
        for handler in logging.getLogger().handlers:
            handler.flush()
        
        with open(log_file_path, 'r') as f:
            content = f.read()
            self.assertIn("Integration test message", content)
            self.assertIn("Integration test error", content)


if __name__ == '__main__':
    unittest.main()
