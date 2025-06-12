"""
Test cases for KrakenMapper
"""

import unittest
import tempfile
import os
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from src.mappers.kraken_mapper import KrakenMapper
from src.mappers.abstract_exchange_mapper import ExchangeInfo
from src.core.configuration_manager import ConfigurationManager


class TestKrakenMapper(unittest.TestCase):
    """Test cases for KrakenMapper class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create mock configuration
        self.mock_config = Mock(spec=ConfigurationManager)
        self.mock_config.get.side_effect = self._mock_config_get
        self.mock_config.getint.return_value = 24
        self.mock_config.getboolean.return_value = True
        self.mock_config.getfloat.return_value = 1.5
        
        # Create temporary file for cache testing
        self.temp_dir = tempfile.mkdtemp()
        self.cache_file = os.path.join(self.temp_dir, "test_mapping.json")
        
        # Create mapper instance
        self.mapper = KrakenMapper(self.mock_config)
        self.mapper.cache_file = self.cache_file
    
    def tearDown(self):
        """Clean up test fixtures"""
        if os.path.exists(self.cache_file):
            os.remove(self.cache_file)
        os.rmdir(self.temp_dir)
    
    def _mock_config_get(self, section, key, fallback=None):
        """Mock configuration getter"""
        config_values = {
            ('MAPPING', 'mapping_file'): self.cache_file,
            ('MAPPING', 'cache_expiry_hours'): '24',
            ('IMPORT', 'rate_limit_delay'): '1.5'
        }
        return config_values.get((section, key), fallback or '')
    
    def test_get_exchange_name(self):
        """Test that mapper returns correct exchange name"""
        self.assertEqual(self.mapper.get_exchange_name(), "kraken")
    
    @patch('requests.Session.get')
    def test_load_kraken_assets_success(self, mock_get):
        """Test successful loading of Kraken assets"""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "error": [],
            "result": {
                "XXBT": {"aclass": "currency", "altname": "XBT", "decimals": 10},
                "XETH": {"aclass": "currency", "altname": "ETH", "decimals": 10}
            }
        }
        mock_get.return_value = mock_response
        
        result = self.mapper._load_kraken_assets()
        
        self.assertTrue(result)
        self.assertEqual(len(self.mapper.assets), 2)
        self.assertIn("XXBT", self.mapper.assets)
        self.assertIn("XETH", self.mapper.assets)
    
    @patch('requests.Session.get')
    def test_load_kraken_assets_api_error(self, mock_get):
        """Test handling of Kraken API error"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "error": ["EGeneral:Internal error"]
        }
        mock_get.return_