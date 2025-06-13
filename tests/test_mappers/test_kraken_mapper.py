"""
Test cases for KrakenMapper with Checkpoint Functionality
"""

import unittest
import tempfile
import os
import json
import time
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import sys

# Add src to path
src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from mappers.kraken_mapper import KrakenMapper, CheckpointData
from mappers.abstract_exchange_mapper import ExchangeInfo
from core.configuration_manager import ConfigurationManager


class TestCheckpointData(unittest.TestCase):
    """Test cases for CheckpointData dataclass"""
    
    def test_checkpoint_data_creation(self):
        """Test creating CheckpointData with defaults"""
        checkpoint = CheckpointData()
        
        self.assertEqual(checkpoint.version, "1.0")
        self.assertEqual(checkpoint.processed_coins, [])
        self.assertEqual(checkpoint.failed_coins, [])
        self.assertEqual(checkpoint.mappings, {})
        self.assertEqual(checkpoint.exchange_data, {})
        self.assertEqual(checkpoint.current_batch, 0)
        self.assertEqual(checkpoint.total_batches, 0)
        self.assertEqual(checkpoint.api_call_count, 0)
    
    def test_checkpoint_data_with_values(self):
        """Test creating CheckpointData with specific values"""
        checkpoint = CheckpointData(
            timestamp="2023-01-01T12:00:00",
            processed_coins=["bitcoin", "ethereum"],
            failed_coins=["failed-coin"],
            current_batch=5,
            total_batches=10,
            api_call_count=25
        )
        
        self.assertEqual(checkpoint.timestamp, "2023-01-01T12:00:00")
        self.assertEqual(len(checkpoint.processed_coins), 2)
        self.assertEqual(len(checkpoint.failed_coins), 1)
        self.assertEqual(checkpoint.current_batch, 5)
        self.assertEqual(checkpoint.total_batches, 10)
        self.assertEqual(checkpoint.api_call_count, 25)


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
        
        # Create temporary directory for checkpoints
        self.temp_dir = tempfile.mkdtemp()
        
        # Create mapper instance
        self.mapper = KrakenMapper(self.mock_config)
        self.mapper.checkpoint_dir = Path(self.temp_dir)
        self.mapper.checkpoint_file = self.mapper.checkpoint_dir / "test_checkpoint.json"
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Clean up checkpoint files
        if self.mapper.checkpoint_file.exists():
            self.mapper.checkpoint_file.unlink()
        
        # Clean up temp directory
        if os.path.exists(self.temp_dir):
            for file in os.listdir(self.temp_dir):
                file_path = os.path.join(self.temp_dir, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            os.rmdir(self.temp_dir)
    
    def _mock_config_get(self, section, key, fallback=None):
        """Mock configuration getter"""
        config_values = {
            ('MAPPING', 'mapping_file'): str(self.mapper.checkpoint_file),
            ('MAPPING', 'cache_expiry_hours'): '24',
            ('IMPORT', 'rate_limit_delay'): '1.5'
        }
        return config_values.get((section, key), fallback or '')
    
    def test_get_exchange_name(self):
        """Test that mapper returns correct exchange name"""
        self.assertEqual(self.mapper.get_exchange_name(), "kraken")
    
    def test_initialize_static_mappings(self):
        """Test that static mappings are properly initialized"""
        self.assertIn('bitcoin', self.mapper.static_mappings)
        self.assertIn('ethereum', self.mapper.static_mappings)
        self.assertEqual(self.mapper.static_mappings['bitcoin']['kraken_symbol'], 'XXBT')
        self.assertEqual(self.mapper.static_mappings['ethereum']['kraken_symbol'], 'XETH')
    
    def test_check_for_existing_checkpoint_no_file(self):
        """Test checking for checkpoint when no file exists"""
        # Ensure checkpoint file doesn't exist
        if self.mapper.checkpoint_file.exists():
            self.mapper.checkpoint_file.unlink()
        
        result = self.mapper.check_for_existing_checkpoint()
        
        self.assertFalse(result)
    
    def test_check_for_existing_checkpoint_valid(self):
        """Test checking for valid checkpoint"""
        # Create valid checkpoint file
        checkpoint_data = {
            "version": "1.0",
            "timestamp": datetime.now().isoformat(),
            "processed_coins": ["bitcoin", "ethereum"],
            "failed_coins": ["failed-coin"],
            "mappings": {"bitcoin": {"kraken_symbol": "XXBT"}},
            "exchange_data": {"XXBT": {"altname": "XBT"}},
            "current_batch": 2,
            "total_batches": 5,
            "api_call_count": 10
        }
        
        with open(self.mapper.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)
        
        result = self.mapper.check_for_existing_checkpoint()
        
        self.assertTrue(result)
        self.assertEqual(len(self.mapper.checkpoint_data.processed_coins), 2)
        self.assertEqual(len(self.mapper.checkpoint_data.failed_coins), 1)
        self.assertEqual(self.mapper.checkpoint_data.api_call_count, 10)
    
    def test_check_for_existing_checkpoint_expired(self):
        """Test checking for expired checkpoint"""
        # Create expired checkpoint file
        old_time = datetime.now() - timedelta(hours=30)  # Older than max_checkpoint_age_hours
        checkpoint_data = {
            "version": "1.0",
            "timestamp": old_time.isoformat(),
            "processed_coins": ["bitcoin"],
            "failed_coins": [],
            "mappings": {},
            "exchange_data": {},
            "current_batch": 0,
            "total_batches": 0,
            "api_call_count": 0
        }
        
        with open(self.mapper.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)
        
        result = self.mapper.check_for_existing_checkpoint()
        
        self.assertFalse(result)
        self.assertFalse(self.mapper.checkpoint_file.exists())  # Should be cleaned up
    
    def test_check_for_existing_checkpoint_invalid_json(self):
        """Test checking for checkpoint with invalid JSON"""
        # Create invalid JSON file
        with open(self.mapper.checkpoint_file, 'w') as f:
            f.write("invalid json content")
        
        result = self.mapper.check_for_existing_checkpoint()
        
        self.assertFalse(result)
        self.assertFalse(self.mapper.checkpoint_file.exists())  # Should be cleaned up
    
    def test_save_checkpoint_force(self):
        """Test forced checkpoint save"""
        # Set some data
        self.mapper.checkpoint_data.processed_coins = ["bitcoin", "ethereum"]
        self.mapper.checkpoint_data.api_call_count = 5
        
        self.mapper.save_checkpoint(force=True)
        
        self.assertTrue(self.mapper.checkpoint_file.exists())
        
        # Verify saved data
        with open(self.mapper.checkpoint_file, 'r') as f:
            saved_data = json.load(f)
        
        self.assertEqual(len(saved_data['processed_coins']), 2)
        self.assertEqual(saved_data['api_call_count'], 5)
        self.assertIn('timestamp', saved_data)
    
    def test_save_checkpoint_interval(self):
        """Test checkpoint save based on interval"""
        # Set checkpoint interval
        self.mapper.checkpoint_interval = 2
        
        # Should not save with 1 processed coin
        self.mapper.checkpoint_data.processed_coins = ["bitcoin"]
        self.mapper.save_checkpoint()
        self.assertFalse(self.mapper.checkpoint_file.exists())
        
        # Should save with 2 processed coins
        self.mapper.checkpoint_data.processed_coins = ["bitcoin", "ethereum"]
        self.mapper.save_checkpoint()
        self.assertTrue(self.mapper.checkpoint_file.exists())
    
    def test_cleanup_checkpoint(self):
        """Test checkpoint cleanup"""
        # Create checkpoint file
        self.mapper.save_checkpoint(force=True)
        self.assertTrue(self.mapper.checkpoint_file.exists())
        
        # Cleanup
        self.mapper._cleanup_checkpoint()
        self.assertFalse(self.mapper.checkpoint_file.exists())
    
    @patch('requests.get')
    def test_make_api_call_success(self, mock_get):
        """Test successful API call"""
        # Mock successful response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"result": {"test": "data"}}
        mock_get.return_value = mock_response
        
        initial_count = self.mapper.checkpoint_data.api_call_count
        
        with patch('time.sleep'):  # Skip actual sleep
            result = self.mapper._make_api_call("https://test.com")
        
        self.assertIsNotNone(result)
        self.assertEqual(result, {"result": {"test": "data"}})
        self.assertEqual(self.mapper.checkpoint_data.api_call_count, initial_count + 1)
        self.assertIsNotNone(self.mapper.checkpoint_data.last_api_call)
    
    @patch('requests.get')
    @patch('time.sleep')
    def test_make_api_call_with_retries(self, mock_sleep, mock_get):
        """Test API call with retries"""
        # Mock failed then successful response
        mock_response_fail = Mock()
        mock_response_fail.raise_for_status.side_effect = Exception("Connection error")
        
        mock_response_success = Mock()
        mock_response_success.raise_for_status.return_value = None
        mock_response_success.json.return_value = {"result": "success"}
        
        mock_get.side_effect = [mock_response_fail, mock_response_success]
        
        result = self.mapper._make_api_call("https://test.com", max_retries=2)
        
        self.assertIsNotNone(result)
        self.assertEqual(result, {"result": "success"})
        self.assertEqual(mock_get.call_count, 2)
        mock_sleep.assert_called()
    
    @patch('requests.get')
    @patch('time.sleep')
    def test_make_api_call_all_retries_fail(self, mock_sleep, mock_get):
        """Test API call when all retries fail"""
        # Mock all responses to fail
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("Connection error")
        mock_get.return_value = mock_response
        
        result = self.mapper._make_api_call("https://test.com", max_retries=2)
        
        self.assertIsNone(result)
        self.assertEqual(mock_get.call_count, 3)  # Initial + 2 retries
    
    def test_get_kraken_assets_cached(self):
        """Test getting Kraken assets from cache"""
        # Set cached exchange data
        cached_data = {"XXBT": {"altname": "XBT"}, "XETH": {"altname": "ETH"}}
        self.mapper.checkpoint_data.exchange_data = cached_data
        
        result = self.mapper._get_kraken_assets()
        
        self.assertEqual(result, cached_data)
    
    @patch.object(KrakenMapper, '_make_api_call')
    def test_get_kraken_assets_api_call(self, mock_api_call):
        """Test getting Kraken assets via API call"""
        # Mock API response
        api_response = {
            "result": {
                "XXBT": {"altname": "XBT", "decimals": 10},
                "XETH": {"altname": "ETH", "decimals": 10}
            }
        }
        mock_api_call.return_value = api_response
        
        result = self.mapper._get_kraken_assets()
        
        self.assertEqual(result, api_response["result"])
        self.assertEqual(self.mapper.checkpoint_data.exchange_data, api_response["result"])
        mock_api_call.assert_called_once()
    
    @patch.object(KrakenMapper, '_make_api_call')
    def test_get_kraken_assets_api_failure(self, mock_api_call):
        """Test getting Kraken assets when API fails"""
        mock_api_call.return_value = None
        
        result = self.mapper._get_kraken_assets()
        
        self.assertEqual(result, {})
    
    @patch.object(KrakenMapper, '_make_api_call')
    def test_get_kraken_ticker_pairs_success(self, mock_api_call):
        """Test getting Kraken ticker pairs successfully"""
        api_response = {
            "result": {
                "XXBTZUSD": {"base": "XXBT", "quote": "ZUSD"},
                "XETHZUSD": {"base": "XETH", "quote": "ZUSD"}
            }
        }
        mock_api_call.return_value = api_response
        
        result = self.mapper._get_kraken_ticker_pairs()
        
        self.assertEqual(result, api_response["result"])
        mock_api_call.assert_called_once()
    
    @patch.object(KrakenMapper, '_make_api_call')
    def test_get_coingecko_exchanges_data_success(self, mock_api_call):
        """Test getting CoinGecko exchange data successfully"""
        api_response = {
            "tickers": [
                {
                    "base": "BTC",
                    "target": "USD", 
                    "market": {"identifier": "kraken"},
                    "trade_url": "https://trade.kraken.com"
                },
                {
                    "base": "BTC",
                    "target": "EUR",
                    "market": {"identifier": "binance"}
                }
            ]
        }
        mock_api_call.return_value = api_response
        
        result = self.mapper._get_coingecko_exchanges_data("bitcoin")
        
        self.assertEqual(len(result), 1)  # Only Kraken ticker should be returned
        self.assertEqual(result[0]["base"], "BTC")
        self.assertEqual(result[0]["market"]["identifier"], "kraken")
    
    def test_create_mapping_for_coin_already_processed(self):
        """Test creating mapping for already processed coin"""
        # Add coin to processed list
        self.mapper.checkpoint_data.processed_coins = ["bitcoin"]
        self.mapper.checkpoint_data.mappings = {"bitcoin": {"kraken_symbol": "XXBT"}}
        
        result = self.mapper._create_mapping_for_coin("bitcoin", {"name": "Bitcoin"})
        
        self.assertEqual(result, {"kraken_symbol": "XXBT"})
    
    def test_create_mapping_for_coin_failed_before(self):
        """Test creating mapping for previously failed coin"""
        # Add coin to failed list
        self.mapper.checkpoint_data.failed_coins = ["failed-coin"]
        
        result = self.mapper._create_mapping_for_coin("failed-coin", {"name": "Failed Coin"})
        
        self.assertIsNone(result)
    
    def test_create_mapping_for_coin_static_mapping(self):
        """Test creating mapping using static mappings"""
        coin_data = {"name": "Bitcoin", "symbol": "btc"}
        
        result = self.mapper._create_mapping_for_coin("bitcoin", coin_data)
        
        self.assertIsNotNone(result)
        self.assertEqual(result["kraken_symbol"], "XXBT")
        self.assertEqual(result["mapping_source"], "static")
        self.assertEqual(result["confidence"], "high")
        self.assertIn("bitcoin", self.mapper.checkpoint_data.processed_coins)
    
    @patch.object(KrakenMapper, '_get_coingecko_exchanges_data')
    def test_create_mapping_for_coin_api_mapping(self, mock_get_exchange_data):
        """Test creating mapping using API data"""
        # Mock CoinGecko exchange data
        mock_get_exchange_data.return_value = [
            {
                "base": "ADA",
                "target": "USD",
                "market": {"identifier": "kraken"}
            }
        ]
        
        coin_data = {"name": "Cardano", "symbol": "ada"}
        
        result = self.mapper._create_mapping_for_coin("cardano", coin_data)
        
        self.assertIsNotNone(result)
        self.assertEqual(result["kraken_symbol"], "ADA")
        self.assertEqual(result["base_currency"], "ADA")
        self.assertEqual(result["target_currency"], "USD")
        self.assertEqual(result["mapping_source"], "api")
        self.assertEqual(result["confidence"], "medium")
        self.assertIn("cardano", self.mapper.checkpoint_data.processed_coins)
    
    @patch.object(KrakenMapper, '_get_coingecko_exchanges_data')
    @patch.object(KrakenMapper, '_get_kraken_assets')
    def test_create_mapping_for_coin_symbol_match(self, mock_get_assets, mock_get_exchange_data):
        """Test creating mapping using symbol matching"""
        # Mock no exchange data from CoinGecko
        mock_get_exchange_data.return_value = None
        
        # Mock Kraken assets
        mock_get_assets.return_value = {
            "MATIC": {"altname": "MATIC", "decimals": 10}
        }
        
        coin_data = {"name": "Polygon", "symbol": "matic"}
        
        result = self.mapper._create_mapping_for_coin("polygon", coin_data)
        
        self.assertIsNotNone(result)
        self.assertEqual(result["kraken_symbol"], "MATIC")
        self.assertEqual(result["mapping_source"], "symbol_match")
        self.assertEqual(result["confidence"], "low")
        self.assertIn("polygon", self.mapper.checkpoint_data.processed_coins)
    
    @patch.object(KrakenMapper, '_get_coingecko_exchanges_data')
    @patch.object(KrakenMapper, '_get_kraken_assets')
    def test_create_mapping_for_coin_no_mapping(self, mock_get_assets, mock_get_exchange_data):
        """Test creating mapping when no mapping can be found"""
        # Mock no data from any source
        mock_get_exchange_data.return_value = None
        mock_get_assets.return_value = {}
        
        coin_data = {"name": "Unknown Coin", "symbol": "unknown"}
        
        result = self.mapper._create_mapping_for_coin("unknown-coin", coin_data)
        
        self.assertIsNone(result)
        self.assertIn("unknown-coin", self.mapper.checkpoint_data.failed_coins)
    
    def test_create_mapping_for_coin_exception(self):
        """Test creating mapping when exception occurs"""
        # Pass invalid coin data to trigger exception
        with patch.object(self.mapper, '_get_coingecko_exchanges_data', side_effect=Exception("API Error")):
            result = self.mapper._create_mapping_for_coin("error-coin", {"name": "Error Coin"})
        
        self.assertIsNone(result)
        self.assertIn("error-coin", self.mapper.checkpoint_data.failed_coins)
    
    @patch.object(KrakenMapper, '_get_kraken_assets')
    def test_build_coin_mapping_fresh_start(self, mock_get_assets):
        """Test building coin mapping from fresh start"""
        mock_get_assets.return_value = {"XXBT": {"altname": "XBT"}}
        
        coin_data = [
            {"id": "bitcoin", "name": "Bitcoin", "symbol": "btc"},
            {"id": "ethereum", "name": "Ethereum", "symbol": "eth"}
        ]
        
        with patch.object(self.mapper, '_create_mapping_for_coin') as mock_create:
            mock_create.side_effect = [
                {"kraken_symbol": "XXBT", "mapping_source": "static"},
                {"kraken_symbol": "XETH", "mapping_source": "static"}
            ]
            
            result = self.mapper.build_coin_mapping(coin_data)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(mock_create.call_count, 2)
    
    def test_build_coin_mapping_resume_from_checkpoint(self):
        """Test building coin mapping resuming from checkpoint"""
        # Create existing checkpoint
        checkpoint_data = {
            "version": "1.0",
            "timestamp": datetime.now().isoformat(),
            "processed_coins": ["bitcoin"],
            "failed_coins": [],
            "mappings": {"bitcoin": {"kraken_symbol": "XXBT"}},
            "exchange_data": {"XXBT": {"altname": "XBT"}},
            "current_batch": 1,
            "total_batches": 2,
            "api_call_count": 5
        }
        
        with open(self.mapper.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)
        
        coin_data = [
            {"id": "bitcoin", "name": "Bitcoin", "symbol": "btc"},
            {"id": "ethereum", "name": "Ethereum", "symbol": "eth"}
        ]
        
        with patch.object(self.mapper, '_create_mapping_for_coin') as mock_create:
            mock_create.return_value = {"kraken_symbol": "XETH", "mapping_source": "static"}
            
            result = self.mapper.build_coin_mapping(coin_data)
        
        # Should resume and only process ethereum (bitcoin already processed)
        self.assertEqual(len(result), 2)  # Both mappings should be present
        self.assertEqual(mock_create.call_count, 1)  # Only ethereum processed
    
    @patch.object(KrakenMapper, 'build_coin_mapping')
    def test_map_coins_to_exchange(self, mock_build_mapping):
        """Test mapping coins to exchange format"""
        # Mock mapping results
        mock_build_mapping.return_value = {
            "bitcoin": {"kraken_symbol": "XXBT", "mapping_source": "static"},
            "ethereum": {"kraken_symbol": "XETH", "mapping_source": "static"}
        }
        
        coin_data = [
            {"id": "bitcoin", "name": "Bitcoin", "symbol": "btc"},
            {"id": "ethereum", "name": "Ethereum", "symbol": "eth"},
            {"id": "dogecoin", "name": "Dogecoin", "symbol": "doge"}  # Not in mappings
        ]
        
        result = self.mapper.map_coins_to_exchange(coin_data)
        
        self.assertEqual(len(result), 3)
        
        # Check mapped coins
        bitcoin_coin = next(coin for coin in result if coin["id"] == "bitcoin")
        self.assertTrue(bitcoin_coin["is_tradeable_on_kraken"])
        self.assertIsNotNone(bitcoin_coin["kraken_data"])
        
        # Check unmapped coin
        dogecoin_coin = next(coin for coin in result if coin["id"] == "dogecoin")
        self.assertFalse(dogecoin_coin["is_tradeable_on_kraken"])
        self.assertIsNone(dogecoin_coin["kraken_data"])
    
    def test_get_exchange_info(self):
        """Test getting exchange information"""
        result = self.mapper.get_exchange_info()
        
        self.assertEqual(result["name"], "Kraken")
        self.assertEqual(result["identifier"], "kraken")
        self.assertIn("checkpoint_support", result)
        self.assertTrue(result["checkpoint_support"])
        self.assertIn("supported_features", result)
    
    @patch.object(KrakenMapper, '_get_kraken_assets')
    def test_validate_mappings_all_valid(self, mock_get_assets):
        """Test validating mappings when all are valid"""
        mock_get_assets.return_value = {
            "XXBT": {"altname": "XBT"},
            "XETH": {"altname": "ETH"}
        }
        
        mappings = {
            "bitcoin": {"base_currency": "XBT"},
            "ethereum": {"base_currency": "ETH"}
        }
        
        result = self.mapper.validate_mappings(mappings)
        
        self.assertEqual(result["valid"], 2)
        self.assertEqual(result["invalid"], 0)
        self.assertEqual(len(result["errors"]), 0)
    
    @patch.object(KrakenMapper, '_get_kraken_assets')
    def test_validate_mappings_some_invalid(self, mock_get_assets):
        """Test validating mappings when some are invalid"""
        mock_get_assets.return_value = {
            "XXBT": {"altname": "XBT"}
        }
        
        mappings = {
            "bitcoin": {"base_currency": "XBT"},
            "ethereum": {"base_currency": "ETH"}  # ETH not in Kraken assets
        }
        
        result = self.mapper.validate_mappings(mappings)
        
        self.assertEqual(result["valid"], 1)
        self.assertEqual(result["invalid"], 1)
        self.assertEqual(len(result["errors"]), 1)
        self.assertIn("ethereum", result["errors"][0])
    
    @patch.object(KrakenMapper, '_get_kraken_assets')
    def test_validate_mappings_api_failure(self, mock_get_assets):
        """Test validating mappings when API fails"""
        mock_get_assets.side_effect = Exception("API Error")
        
        mappings = {"bitcoin": {"base_currency": "XBT"}}
        
        result = self.mapper.validate_mappings(mappings)
        
        self.assertEqual(len(result["errors"]), 1)
        self.assertIn("Failed to fetch Kraken data", result["errors"][0])
    
    def test_get_checkpoint_status_no_checkpoint(self):
        """Test getting checkpoint status when no checkpoint exists"""
        result = self.mapper.get_checkpoint_status()
        
        self.assertFalse(result["checkpoint_exists"])
        self.assertEqual(result["processed_coins"], 0)
        self.assertEqual(result["failed_coins"], 0)
        self.assertEqual(result["successful_mappings"], 0)
        self.assertEqual(result["api_calls_made"], 0)
    
    def test_get_checkpoint_status_with_checkpoint(self):
        """Test getting checkpoint status with existing checkpoint"""
        # Set checkpoint data
        self.mapper.checkpoint_data.processed_coins = ["bitcoin", "ethereum"]
        self.mapper.checkpoint_data.failed_coins = ["failed-coin"]
        self.mapper.checkpoint_data.mappings = {"bitcoin": {"kraken_symbol": "XXBT"}}
        self.mapper.checkpoint_data.current_batch = 3
        self.mapper.checkpoint_data.total_batches = 10
        self.mapper.checkpoint_data.api_call_count = 25
        self.mapper.checkpoint_data.timestamp = "2023-01-01T12:00:00"
        self.mapper.checkpoint_data.last_api_call = "2023-01-01T12:30:00"
        
        # Save checkpoint to create file
        self.mapper.save_checkpoint(force=True)
        
        result = self.mapper.get_checkpoint_status()
        
        self.assertTrue(result["checkpoint_exists"])
        self.assertEqual(result["processed_coins"], 2)
        self.assertEqual(result["failed_coins"], 1)
        self.assertEqual(result["successful_mappings"], 1)
        self.assertEqual(result["current_batch"], 3)
        self.assertEqual(result["total_batches"], 10)
        self.assertEqual(result["api_calls_made"], 25)
        self.assertEqual(result["last_checkpoint"], "2023-01-01T12:00:00")
        self.assertEqual(result["last_api_call"], "2023-01-01T12:30:00")


class TestKrakenMapperIntegration(unittest.TestCase):
    """Integration tests for KrakenMapper"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create real configuration
        config_content = f"""
[MAPPING]
mapping_file = {self.temp_dir}/kraken_mapping.json
cache_expiry_hours = 24
rebuild_mapping_days = 7

[IMPORT]
rate_limit_delay = 0.1
"""
        
        config_path = os.path.join(self.temp_dir, "config.ini")
        with open(config_path, 'w') as f:
            f.write(config_content)
        
        from core.configuration_manager import ConfigurationManager
        self.config = ConfigurationManager(config_path)
        
        self.mapper = KrakenMapper(self.config)
        self.mapper.checkpoint_dir = Path(self.temp_dir)
        self.mapper.checkpoint_file = self.mapper.checkpoint_dir / "integration_checkpoint.json"
        self.mapper.api_delay = 0.1  # Speed up tests
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Clean up files
        for file in os.listdir(self.temp_dir):
            file_path = os.path.join(self.temp_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        os.rmdir(self.temp_dir)
    
    def test_checkpoint_persistence_across_instances(self):
        """Test that checkpoint data persists across mapper instances"""
        # Set up first mapper instance with some data
        mapper1 = KrakenMapper(self.config)
        mapper1.checkpoint_dir = Path(self.temp_dir)
        mapper1.checkpoint_file = self.mapper.checkpoint_file
        
        mapper1.checkpoint_data.processed_coins = ["bitcoin", "ethereum"]
        mapper1.checkpoint_data.api_call_count = 10
        mapper1.save_checkpoint(force=True)
        
        # Create second mapper instance
        mapper2 = KrakenMapper(self.config)
        mapper2.checkpoint_dir = Path(self.temp_dir)
        mapper2.checkpoint_file = self.mapper.checkpoint_file
        
        # Load checkpoint
        result = mapper2.check_for_existing_checkpoint()
        
        self.assertTrue(result)
        self.assertEqual(len(mapper2.checkpoint_data.processed_coins), 2)
        self.assertEqual(mapper2.checkpoint_data.api_call_count, 10)
    
    @patch.object(KrakenMapper, '_make_api_call')
    def test_batch_processing_with_checkpoints(self, mock_api_call):
        """Test batch processing with checkpoint saves"""
        # Mock API responses
        mock_api_call.return_value = {"result": {"XXBT": {"altname": "XBT"}}}
        
        # Create test coin data
        coin_data = [
            {"id": f"coin-{i}", "name": f"Coin {i}", "symbol": f"c{i}"}
            for i in range(10)
        ]
        
        # Set small checkpoint interval for testing
        self.mapper.checkpoint_interval = 3
        
        # Mock the mapping creation to avoid actual API calls
        def mock_create_mapping(coin_id, coin_data):
            self.mapper.checkpoint_data.processed_coins.append(coin_id)
            self.mapper.checkpoint_data.mappings[coin_id] = {
                "kraken_symbol": f"MOCK_{coin_id.upper()}",
                "mapping_source": "test"
            }
            return self.mapper.checkpoint_data.mappings[coin_id]
        
        with patch.object(self.mapper, '_create_mapping_for_coin', side_effect=mock_create_mapping):
            result = self.mapper.build_coin_mapping(coin_data)
        
        # Verify all coins were processed
        self.assertEqual(len(result), 10)
        self.assertEqual(len(self.mapper.checkpoint_data.processed_coins), 10)
        
        # Verify checkpoint was created and cleaned up
        self.assertFalse(self.mapper.checkpoint_file.exists())  # Should be cleaned up after completion
    
    def test_error_handling_and_recovery(self):
        """Test error handling and recovery capabilities"""
        coin_data = [
            {"id": "bitcoin", "name": "Bitcoin", "symbol": "btc"},
            {"id": "error-coin", "name": "Error Coin", "symbol": "error"},
            {"id": "ethereum", "name": "Ethereum", "symbol": "eth"}
        ]
        
        def mock_create_mapping(coin_id, coin_data):
            if coin_id == "error-coin":
                raise Exception("Simulated API error")
            
            self.mapper.checkpoint_data.processed_coins.append(coin_id)
            mapping = {"kraken_symbol": f"MOCK_{coin_id.upper()}", "mapping_source": "test"}
            self.mapper.checkpoint_data.mappings[coin_id] = mapping
            return mapping
        
        with patch.object(self.mapper, '_get_kraken_assets', return_value={}):
            with patch.object(self.mapper, '_create_mapping_for_coin', side_effect=mock_create_mapping):
                result = self.mapper.build_coin_mapping(coin_data)
        
        # Should have processed 2 coins successfully, 1 failed
        self.assertEqual(len(result), 2)
        self.assertEqual(len(self.mapper.checkpoint_data.processed_coins), 2)
        self.assertEqual(len(self.mapper.checkpoint_data.failed_coins), 1)
        self.assertIn("error-coin", self.mapper.checkpoint_data.failed_coins)
    
    def test_interrupt_and_resume_simulation(self):
        """Test simulated interrupt and resume functionality"""
        coin_data = [
            {"id": f"coin-{i}", "name": f"Coin {i}", "symbol": f"c{i}"}
            for i in range(10)
        ]
        
        # First run: process half the coins then "interrupt"
        self.mapper.checkpoint_interval = 2
        
        def mock_create_mapping_interrupt(coin_id, coin_data):
            self.mapper.checkpoint_data.processed_coins.append(coin_id)
            mapping = {"kraken_symbol": f"MOCK_{coin_id.upper()}", "mapping_source": "test"}
            self.mapper.checkpoint_data.mappings[coin_id] = mapping
            
            # Simulate interrupt after processing 5 coins
            if len(self.mapper.checkpoint_data.processed_coins) == 5:
                self.mapper.save_checkpoint(force=True)
                raise KeyboardInterrupt("Simulated interrupt")
            
            return mapping
        
        with patch.object(self.mapper, '_get_kraken_assets', return_value={}):
            with patch.object(self.mapper, '_create_mapping_for_coin', side_effect=mock_create_mapping_interrupt):
                with self.assertRaises(KeyboardInterrupt):
                    self.mapper.build_coin_mapping(coin_data)
        
        # Verify checkpoint was saved
        self.assertTrue(self.mapper.checkpoint_file.exists())
        self.assertEqual(len(self.mapper.checkpoint_data.processed_coins), 5)
        
        # Second run: resume from checkpoint
        mapper2 = KrakenMapper(self.config)
        mapper2.checkpoint_dir = Path(self.temp_dir)
        mapper2.checkpoint_file = self.mapper.checkpoint_file
        
        def mock_create_mapping_resume(coin_id, coin_data):
            mapper2.checkpoint_data.processed_coins.append(coin_id)
            mapping = {"kraken_symbol": f"MOCK_{coin_id.upper()}", "mapping_source": "test"}
            mapper2.checkpoint_data.mappings[coin_id] = mapping
            return mapping
        
        with patch.object(mapper2, '_get_kraken_assets', return_value={}):
            with patch.object(mapper2, '_create_mapping_for_coin', side_effect=mock_create_mapping_resume):
                result = mapper2.build_coin_mapping(coin_data)
        
        # Should have processed all 10 coins (5 from checkpoint + 5 new)
        self.assertEqual(len(result), 10)
        self.assertEqual(len(mapper2.checkpoint_data.processed_coins), 10)
        
        # Checkpoint should be cleaned up after completion
        self.assertFalse(mapper2.checkpoint_file.exists())


if __name__ == '__main__':
    unittest.main()
