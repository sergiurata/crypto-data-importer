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
        self.temp_dir = tempfile.mkdtemp()
        self.test_config_path = os.path.join(self.temp_dir, "test_config.ini")
        
    def tearDown(self):
        """Clean up test fixtures"""
        if os.path.exists(self.test_config_path):
            os.remove(self.test_config_path)
        os.rmdir(self.temp_dir)
    
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
    
    @patch('builtins.open', side_effect=IOError("Permission denied"))
    def test_save_config_failure(self, mock_open):
        """Test handling of save configuration failure"""
        config_manager = ConfigurationManager(self.test_config_path)
        
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


if __name__ == '__main__':
    unittest.main()
