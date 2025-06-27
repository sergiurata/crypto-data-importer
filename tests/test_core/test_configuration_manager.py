"""
Test cases for ConfigurationManager
"""

import unittest
import tempfile
import os
from unittest.mock import patch, mock_open
import configparser
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from core.configuration_manager import ConfigurationManager


class TestConfigurationManager(unittest.TestCase):
    """Test cases for ConfigurationManager class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Use current directory for tests to stay within allowed directories
        self.temp_dir = Path.cwd() / "test_temp_main"
        self.temp_dir.mkdir(exist_ok=True)
        self.test_config_path = str(self.temp_dir / "test_config.ini")
        
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        
        # Clean up the main temp directory
        if self.temp_dir.exists():
            try:
                shutil.rmtree(self.temp_dir)
            except (OSError, PermissionError):
                # Try individual file cleanup if directory removal fails
                try:
                    for file_path in self.temp_dir.rglob("*"):
                        if file_path.is_file():
                            file_path.unlink(missing_ok=True)
                    self.temp_dir.rmdir()
                except (OSError, PermissionError):
                    pass  # Best effort cleanup
        
        # Clean up any stray test files in current directory
        current_dir = Path.cwd()
        
        # Remove any .tmp files
        for tmp_file in current_dir.glob("*.tmp"):
            try:
                tmp_file.unlink(missing_ok=True)
            except (OSError, PermissionError):
                pass
    
    def test_init_with_default_path(self):
        """Test initialization with default config path"""
        with patch('os.path.exists', return_value=False):
            with patch('builtins.open', mock_open()):
                config_manager = ConfigurationManager()
                self.assertEqual(config_manager.config_path, "config.ini")
    
    def test_init_with_custom_path(self):
        """Test initialization with custom config path"""
        with patch('os.path.exists', return_value=False):
            with patch('builtins.open', mock_open()):
                config_manager = ConfigurationManager(self.test_config_path)
                self.assertEqual(config_manager.config_path, self.test_config_path)
    
    def test_create_default_config(self):
        """Test creation of default configuration file"""
        config_manager = ConfigurationManager(self.test_config_path)
        
        self.assertTrue(os.path.exists(self.test_config_path))
        
        # Verify config file contains expected sections
        created_config = configparser.ConfigParser()
        created_config.read(self.test_config_path)
        
        expected_sections = ['DATABASE', 'IMPORT', 'MAPPING', 'FILTERING', 'UPDATES', 'LOGGING', 'API', 'PROVIDERS']
        for section in expected_sections:
            self.assertIn(section, created_config.sections())
    
    def test_get_default_config(self):
        """Test that default config contains required sections"""
        config_manager = ConfigurationManager(self.test_config_path)
        default_config = config_manager._get_default_config()
        
        # Check required sections exist
        required_sections = ['DATABASE', 'IMPORT', 'MAPPING']
        for section in required_sections:
            self.assertIn(section, default_config)
        
        # Check some specific default values
        self.assertEqual(default_config['IMPORT']['max_coins'], '500')
        self.assertEqual(default_config['DATABASE']['create_if_not_exists'], 'true')
    
    def test_load_existing_config(self):
        """Test loading an existing configuration file"""
        # Create a test config file
        test_config_content = """
[DATABASE]
database_path = /test/path/test.adb
create_if_not_exists = false

[IMPORT]
max_coins = 100
min_market_cap = 5000000
"""
        
        with open(self.test_config_path, 'w') as f:
            f.write(test_config_content)
        
        config_manager = ConfigurationManager(self.test_config_path)
        
        # Verify values were loaded correctly
        self.assertEqual(config_manager.get('DATABASE', 'database_path'), '/test/path/test.adb')
        self.assertFalse(config_manager.getboolean('DATABASE', 'create_if_not_exists'))
        self.assertEqual(config_manager.getint('IMPORT', 'max_coins'), 100)
        self.assertEqual(config_manager.getfloat('IMPORT', 'min_market_cap'), 5000000.0)
    
    def test_validate_config_adds_missing_sections(self):
        """Test that validation adds missing sections"""
        # Create incomplete config
        incomplete_config = """
[DATABASE]
database_path = /test/path/test.adb
"""
        
        with open(self.test_config_path, 'w') as f:
            f.write(incomplete_config)
        
        config_manager = ConfigurationManager(self.test_config_path)
        
        # Verify missing sections were added
        required_sections = ['IMPORT', 'MAPPING', 'PROVIDERS']
        for section in required_sections:
            self.assertTrue(config_manager.config.has_section(section))
    
    def test_get_methods(self):
        """Test various get methods"""
        config_manager = ConfigurationManager(self.test_config_path)
        
        # Test get() method
        self.assertIsInstance(config_manager.get('DATABASE', 'database_path'), str)
        
        # Test getint() method
        max_coins = config_manager.getint('IMPORT', 'max_coins')
        self.assertIsInstance(max_coins, int)
        self.assertEqual(max_coins, 500)
        
        # Test getfloat() method
        market_cap = config_manager.getfloat('IMPORT', 'min_market_cap')
        self.assertIsInstance(market_cap, float)
        
        # Test getboolean() method
        create_db = config_manager.getboolean('DATABASE', 'create_if_not_exists')
        self.assertIsInstance(create_db, bool)
        self.assertTrue(create_db)
    
    def test_getlist_method(self):
        """Test getlist method with various inputs"""
        config_manager = ConfigurationManager(self.test_config_path)
        
        # Test empty list
        empty_list = config_manager.getlist('FILTERING', 'excluded_symbols')
        self.assertEqual(empty_list, [])
        
        # Test with actual values
        config_manager.set_value('FILTERING', 'excluded_symbols', 'BTC,ETH,LTC')
        symbol_list = config_manager.getlist('FILTERING', 'excluded_symbols')
        self.assertEqual(symbol_list, ['BTC', 'ETH', 'LTC'])
        
        # Test with spaces
        config_manager.set_value('FILTERING', 'excluded_symbols', 'BTC, ETH , LTC ')
        symbol_list = config_manager.getlist('FILTERING', 'excluded_symbols')
        self.assertEqual(symbol_list, ['BTC', 'ETH', 'LTC'])
    
    def test_set_value_method(self):
        """Test setting configuration values"""
        config_manager = ConfigurationManager(self.test_config_path)
        
        # Test setting new value
        config_manager.set_value('TEST', 'test_key', 'test_value')
        self.assertEqual(config_manager.get('TEST', 'test_key'), 'test_value')
        
        # Test overwriting existing value
        config_manager.set_value('IMPORT', 'max_coins', '1000')
        self.assertEqual(config_manager.getint('IMPORT', 'max_coins'), 1000)
    
    def test_save_config(self):
        """Test saving configuration to file"""
        config_manager = ConfigurationManager(self.test_config_path)
        
        # Modify a value
        config_manager.set_value('IMPORT', 'max_coins', '750')
        
        # Save configuration
        config_manager.save_config()
        
        # Create new instance to verify save worked
        new_config_manager = ConfigurationManager(self.test_config_path)
        self.assertEqual(new_config_manager.getint('IMPORT', 'max_coins'), 750)
    
    def test_fallback_values(self):
        """Test fallback values for missing configurations"""
        config_manager = ConfigurationManager(self.test_config_path)
        
        # Test fallback for non-existent key
        fallback_value = config_manager.get('NONEXISTENT', 'nonexistent_key', 'fallback')
        self.assertEqual(fallback_value, 'fallback')
        
        # Test fallback for getint
        fallback_int = config_manager.getint('NONEXISTENT', 'nonexistent_key', 42)
        self.assertEqual(fallback_int, 42)
        
        # Test fallback for getfloat
        fallback_float = config_manager.getfloat('NONEXISTENT', 'nonexistent_key', 3.14)
        self.assertEqual(fallback_float, 3.14)
        
        # Test fallback for getboolean
        fallback_bool = config_manager.getboolean('NONEXISTENT', 'nonexistent_key', True)
        self.assertTrue(fallback_bool)
    
    def test_save_config_failure(self):
        """Test handling of save configuration failure"""
        # Create config manager first
        config_manager = ConfigurationManager(self.test_config_path)
        
        # Then patch just the save_config method to simulate failure
        with patch('builtins.open', side_effect=IOError("Permission denied")):
            # This should not raise an exception
            config_manager.save_config()
    
    def test_print_config(self):
        """Test print_config method"""
        config_manager = ConfigurationManager(self.test_config_path)
        
        # This should not raise an exception
        with patch('core.configuration_manager.logger') as mock_logger:
            config_manager.print_config()
            mock_logger.info.assert_called()
    
    def test_generate_config_with_comments(self):
        """Test that generated config includes comments"""
        config_manager = ConfigurationManager(self.test_config_path)
        config_content = config_manager._generate_config_with_comments()
        
        # Verify comments are included
        self.assertIn('# CoinGecko AmiBroker Importer Configuration', config_content)
        self.assertIn('# Path to AmiBroker database file', config_content)
        self.assertIn('[DATABASE]', config_content)
        self.assertIn('[IMPORT]', config_content)
    
    def test_load_defaults_on_error(self):
        """Test that defaults are loaded when config loading fails"""
        # Create invalid config file
        with open(self.test_config_path, 'w') as f:
            f.write("Invalid config content [[[")
        
        with patch('core.configuration_manager.logger') as mock_logger:
            config_manager = ConfigurationManager(self.test_config_path)
            
            # Should still have default values
            self.assertTrue(config_manager.config.has_section('DATABASE'))
            mock_logger.error.assert_called()


class TestConfigurationManagerSecurity(unittest.TestCase):
    """CVE-002 Security test cases for ConfigurationManager path traversal prevention"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Use current directory for tests to stay within allowed directories
        self.temp_dir = Path.cwd() / "test_temp"
        self.temp_dir.mkdir(exist_ok=True)
        self.safe_config_path = str(self.temp_dir / "safe_config.ini")
        
        # Store original config.ini content to protect it during tests
        config_ini_path = Path.cwd() / "config.ini"
        self.original_config_exists = config_ini_path.exists()
        if self.original_config_exists:
            self.original_config_content = config_ini_path.read_text()
        else:
            self.original_config_content = None
        
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        import glob
        
        # Clean up the main temp directory
        if self.temp_dir.exists():
            try:
                shutil.rmtree(self.temp_dir)
            except (OSError, PermissionError) as e:
                # Try individual file cleanup if directory removal fails
                try:
                    for file_path in self.temp_dir.rglob("*"):
                        if file_path.is_file():
                            file_path.unlink(missing_ok=True)
                    self.temp_dir.rmdir()
                except (OSError, PermissionError):
                    pass  # Best effort cleanup
        
        # Clean up any stray test files in current directory
        current_dir = Path.cwd()
        
        # Remove any .tmp files
        for tmp_file in current_dir.glob("*.tmp"):
            try:
                tmp_file.unlink(missing_ok=True)
            except (OSError, PermissionError):
                pass
        
        # Remove any test config files (but not the main config.ini)
        test_config_patterns = [
            "test_*.ini",
            "*test*.ini", 
            "safe_config*.ini",
            "sample_config.ini"
        ]
        
        for pattern in test_config_patterns:
            for test_file in current_dir.glob(pattern):
                # Make sure we don't delete the main config.ini
                if test_file.name != "config.ini":
                    try:
                        test_file.unlink(missing_ok=True)
                    except (OSError, PermissionError):
                        pass
        
        # CRITICAL: Clean up any dangerous filenames that might have been created
        dangerous_patterns = ["C:*", "*Windows*", "*System32*", "*etc*passwd*"]
        for pattern in dangerous_patterns:
            for dangerous_file in current_dir.glob(pattern):
                if dangerous_file.is_file():
                    try:
                        dangerous_file.unlink()
                    except (OSError, PermissionError):
                        pass
        
        # Clean up config directory if it was created during tests
        config_dir = current_dir / "config"
        if config_dir.exists():
            try:
                # Remove any test files from config directory
                for test_file in config_dir.glob("*"):
                    if test_file.is_file() and "test" in test_file.name.lower():
                        test_file.unlink()
                    elif test_file.is_file() and any(dangerous in test_file.name.lower() 
                                                   for dangerous in ["malicious", "windows", "system32", "etc", "passwd"]):
                        test_file.unlink()
                
                # Remove config directory if it's empty or only contains test files
                remaining_files = list(config_dir.glob("*"))
                if not remaining_files:
                    config_dir.rmdir()
            except (OSError, PermissionError):
                pass
        
        # Restore original config.ini if it was modified during tests
        config_ini_path = Path.cwd() / "config.ini"
        if self.original_config_exists and self.original_config_content:
            try:
                current_content = config_ini_path.read_text() if config_ini_path.exists() else ""
                if current_content != self.original_config_content:
                    config_ini_path.write_text(self.original_config_content)
            except (OSError, PermissionError):
                pass
        elif not self.original_config_exists and config_ini_path.exists():
            # Remove config.ini if it didn't exist before and was created during tests
            try:
                config_ini_path.unlink()
            except (OSError, PermissionError):
                pass
    
    def test_cve002_path_traversal_prevention_unix(self):
        """Test CVE-002: Path traversal prevention with Unix-style paths"""
        dangerous_paths = [
            "../../../etc/passwd",
            "../../root/.ssh/id_rsa", 
            "../../../tmp/malicious.ini",
            "../../../../etc/shadow",
            "../../../home/user/.bashrc"
        ]
        
        for dangerous_path in dangerous_paths:
            with self.subTest(path=dangerous_path):
                with patch('core.configuration_manager.logger') as mock_logger:
                    # Should not raise exception, but should use fallback
                    config_manager = ConfigurationManager(dangerous_path)
                    
                    # Should have logged the security violation
                    mock_logger.error.assert_called()
                    
                    # Should have used fallback path, not the dangerous one
                    self.assertNotEqual(config_manager.config_path, dangerous_path)
                    self.assertTrue(config_manager.config_path.endswith('config.ini'))
    
    def test_cve002_path_traversal_prevention_windows(self):
        """Test CVE-002: Path traversal prevention with Windows-style paths"""
        dangerous_paths = [
            "..\\..\\..\\Windows\\System32\\config\\system",
            "..\\..\\Users\\Administrator\\Desktop\\secrets.txt",
            "..\\..\\..\\Windows\\System32\\drivers\\etc\\hosts",
            "C:\\Windows\\System32\\cmd.exe",
            "..\\..\\..\\Program Files\\sensitive.ini"
        ]
        
        for dangerous_path in dangerous_paths:
            with self.subTest(path=dangerous_path):
                with patch('core.configuration_manager.logger') as mock_logger:
                    config_manager = ConfigurationManager(dangerous_path)
                    
                    # Should have logged the security violation
                    mock_logger.error.assert_called()
                    
                    # Should have used fallback path
                    self.assertNotEqual(config_manager.config_path, dangerous_path)
                    self.assertTrue(config_manager.config_path.endswith('config.ini'))
    
    def test_cve002_dangerous_filename_patterns(self):
        """Test CVE-002: Dangerous filename pattern detection"""
        dangerous_filenames = [
            "config..ini",
            "config<script>.ini",
            "config>output.ini",
            "config|pipe.ini",
            "config*wildcard.ini",
            "config?query.ini"
        ]
        
        for dangerous_filename in dangerous_filenames:
            with self.subTest(filename=dangerous_filename):
                with patch('core.configuration_manager.logger') as mock_logger:
                    config_manager = ConfigurationManager(dangerous_filename)
                    
                    # Should have logged the security violation
                    mock_logger.error.assert_called()
                    
                    # Should have used fallback path
                    self.assertNotEqual(config_manager.config_path, dangerous_filename)
    
    def test_cve002_absolute_path_outside_allowed_dirs(self):
        """Test CVE-002: Absolute paths outside allowed directories"""
        dangerous_absolute_paths = [
            "/etc/passwd",
            "/root/.ssh/id_rsa",
            "/tmp/malicious.ini",
            "/var/log/system.log",
            "/home/user/.bashrc"
        ]
        
        for dangerous_path in dangerous_absolute_paths:
            with self.subTest(path=dangerous_path):
                with patch('core.configuration_manager.logger') as mock_logger:
                    config_manager = ConfigurationManager(dangerous_path)
                    
                    # Should have logged the security violation
                    mock_logger.error.assert_called()
                    
                    # Should have used fallback path
                    self.assertNotEqual(config_manager.config_path, dangerous_path)
    
    def test_cve002_valid_paths_allowed(self):
        """Test CVE-002: Valid paths within allowed directories are accepted"""
        # Test valid paths that should be allowed
        valid_paths = [
            "config.ini",
            "./config.ini",
            "config/app.ini",
            "my_config.ini"
        ]
        
        for valid_path in valid_paths:
            with self.subTest(path=valid_path):
                with patch('os.path.exists', return_value=False):
                    with patch('builtins.open', mock_open()):
                        config_manager = ConfigurationManager(valid_path)
                        
                        # Should accept valid paths
                        resolved_path = Path(valid_path).resolve()
                        self.assertEqual(config_manager.config_path, str(resolved_path))
    
    def test_cve002_symlink_attack_prevention(self):
        """Test CVE-002: Symlink attack prevention"""
        # Create a symlink that points outside allowed directory
        symlink_path = os.path.join(self.temp_dir, "symlink_config.ini")
        
        # Create target outside allowed directory
        target_path = "/tmp/sensitive_file.txt"
        
        # Mock symlink creation and resolution
        with patch('pathlib.Path.resolve') as mock_resolve:
            mock_resolve.return_value = Path(target_path)
            
            with patch('core.configuration_manager.logger') as mock_logger:
                config_manager = ConfigurationManager(symlink_path)
                
                # Should have detected and blocked the symlink attack
                mock_logger.error.assert_called()
                
                # Should have used fallback path
                self.assertNotEqual(config_manager.config_path, target_path)
    
    def test_cve002_secure_file_write_validation(self):
        """Test CVE-002: Secure file write operation validation"""
        config_manager = ConfigurationManager(self.safe_config_path)
        
        # Test secure write with valid path
        test_content = "test configuration content"
        result = config_manager._secure_file_write(self.safe_config_path, test_content)
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.safe_config_path))
        
        # Verify content was written correctly
        with open(self.safe_config_path, 'r') as f:
            written_content = f.read()
        self.assertEqual(written_content, test_content)
    
    def test_cve002_secure_file_write_atomic_operation(self):
        """Test CVE-002: Secure file write uses atomic operations"""
        config_manager = ConfigurationManager(self.safe_config_path)
        
        # Mock to simulate interruption during write
        with patch('pathlib.Path.rename', side_effect=OSError("Simulated interruption")):
            result = config_manager._secure_file_write(self.safe_config_path, "test content")
            
            # Should fail gracefully
            self.assertFalse(result)
            
            # Original file should not be corrupted (atomic operation)
            self.assertFalse(os.path.exists(self.safe_config_path))
    
    def test_cve002_create_default_config_security(self):
        """Test CVE-002: create_default_config uses secure operations"""
        with patch.object(ConfigurationManager, '_secure_file_write') as mock_secure_write:
            mock_secure_write.return_value = True
            
            config_manager = ConfigurationManager(self.safe_config_path)
            
            # Should have called secure file write
            mock_secure_write.assert_called()
    
    def test_cve002_save_config_security(self):
        """Test CVE-002: save_config uses secure operations"""
        config_manager = ConfigurationManager(self.safe_config_path)
        
        with patch.object(config_manager, '_secure_file_write') as mock_secure_write:
            mock_secure_write.return_value = True
            
            config_manager.save_config()
            
            # Should have called secure file write
            mock_secure_write.assert_called()
    
    def test_cve002_file_extension_validation(self):
        """Test CVE-002: File extension validation warnings"""
        unusual_extensions = [
            "config.txt",
            "config.json", 
            "config.exe",
            "config.sh",
            "config.bat"
        ]
        
        for unusual_ext in unusual_extensions:
            config_path = str(self.temp_dir / unusual_ext)
            with self.subTest(extension=unusual_ext):
                with patch('core.configuration_manager.logger') as mock_logger:
                    with patch('os.path.exists', return_value=False):
                        with patch('builtins.open', mock_open()):
                            ConfigurationManager(config_path)
                            
                            # Should log warning for unusual extension
                            mock_logger.warning.assert_called()
    
    def test_cve002_defense_in_depth_validation(self):
        """Test CVE-002: Defense in depth - multiple validation layers"""
        # Test that even if initial validation is bypassed, 
        # secure file operations still validate paths
        config_manager = ConfigurationManager(self.safe_config_path)
        
        # Attempt to write to dangerous path directly
        dangerous_path = "../../../etc/passwd"
        
        with patch('core.configuration_manager.logger') as mock_logger:
            result = config_manager._secure_file_write(dangerous_path, "malicious content")
            
            # Should fail due to path validation in secure_file_write
            self.assertFalse(result)
            mock_logger.error.assert_called()


def cleanup_all_test_files():
    """Module-level cleanup function to remove any remaining test files"""
    import shutil
    from pathlib import Path
    import os
    
    current_dir = Path.cwd()
    
    # Clean up test directories
    test_dirs = [
        "test_temp",
        "test_temp_main", 
        "test_temp_security"
    ]
    
    for dir_name in test_dirs:
        test_dir = current_dir / dir_name
        if test_dir.exists():
            try:
                shutil.rmtree(test_dir)
                print(f"Cleaned up directory: {test_dir}")
            except (OSError, PermissionError) as e:
                print(f"Warning: Could not remove {test_dir}: {e}")
    
    # Clean up temporary files
    temp_patterns = [
        "*.tmp",
        "test_*.ini",
        "*test*.ini",
        "safe_config*.ini",
        "sample_config.ini"
    ]
    
    for pattern in temp_patterns:
        for temp_file in current_dir.glob(pattern):
            # Protect the main config.ini
            if temp_file.name != "config.ini":
                try:
                    temp_file.unlink(missing_ok=True)
                    print(f"Cleaned up file: {temp_file}")
                except (OSError, PermissionError) as e:
                    print(f"Warning: Could not remove {temp_file}: {e}")
    
    # CRITICAL: Clean up dangerous Windows-style filenames that may have been created
    dangerous_patterns = [
        "C:*",
        "*Windows*",
        "*System32*",
        "*Program Files*",
        "*etc*passwd*",
        "*root*"
    ]
    
    for pattern in dangerous_patterns:
        for dangerous_file in current_dir.glob(pattern):
            # Extra safety check - only remove if it looks like a test artifact
            if dangerous_file.is_file():
                try:
                    dangerous_file.unlink()
                    print(f"SECURITY: Removed dangerous test file: {dangerous_file}")
                except (OSError, PermissionError) as e:
                    print(f"ERROR: Could not remove dangerous file {dangerous_file}: {e}")
    
    # Also clean up files that contain system paths in their names
    for file_path in current_dir.iterdir():
        if file_path.is_file():
            filename_lower = file_path.name.lower()
            dangerous_keywords = ['windows', 'system32', 'etc', 'passwd', 'shadow', 'cmd.exe']
            
            if any(keyword in filename_lower for keyword in dangerous_keywords):
                # This looks like a test artifact with a dangerous name
                if filename_lower != "config.ini":  # Protect main config
                    try:
                        file_path.unlink()
                        print(f"SECURITY: Removed file with suspicious name: {file_path}")
                    except (OSError, PermissionError) as e:
                        print(f"ERROR: Could not remove suspicious file {file_path}: {e}")
    
    # Clean up config directory if it was created during tests
    config_dir = current_dir / "config"
    if config_dir.exists():
        try:
            # Remove any test files from config directory
            test_files_removed = []
            for test_file in config_dir.glob("*"):
                if test_file.is_file():
                    filename_lower = test_file.name.lower()
                    # Remove files that look like test artifacts
                    if any(keyword in filename_lower for keyword in 
                          ["test", "malicious", "windows", "system32", "etc", "passwd", "shadow", "cmd.exe"]):
                        test_file.unlink()
                        test_files_removed.append(test_file.name)
            
            if test_files_removed:
                print(f"Cleaned up test files from config/: {test_files_removed}")
            
            # Remove config directory if it's empty
            remaining_files = list(config_dir.glob("*"))
            if not remaining_files:
                config_dir.rmdir()
                print("Removed empty config/ directory created during tests")
        except (OSError, PermissionError) as e:
            print(f"Warning: Could not clean up config directory: {e}")


if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        # Ensure cleanup runs even if tests fail
        cleanup_all_test_files()
