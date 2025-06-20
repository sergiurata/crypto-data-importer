"""
Test cases for KrakenMapper with Checkpoint/Resume Functionality
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

from mappers.kraken_mapper import KrakenMapper
from mappers.abstract_exchange_mapper import ExchangeInfo
from core.configuration_manager import ConfigurationManager


class TestKrakenMapperCheckpoints(unittest.TestCase):
    """Test cases for KrakenMapper checkpoint functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create mock configuration
        self.mock_config = Mock(spec=ConfigurationManager)
        self.mock_config.get.side_effect = self._mock_config_get
        self.mock_config.getint.return_value = 100
        self.mock_config.getboolean.return_value = True
        self.mock_config.getfloat.return_value = 1.5
        
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        self.checkpoint_file = os.path.join(self.temp_dir, "test_checkpoint.json")
        self.cache_file = os.path.join(self.temp_dir, "test_cache.json")
        
        # Create mapper instance
        self.mapper = KrakenMapper(self.mock_config)
        self.mapper.checkpoint_file = self.checkpoint_file
        self.mapper.cache_file = self.cache_file
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Clean up temp files
        for file in [self.checkpoint_file, self.cache_file]:
            if os.path.exists(file):
                os.remove(file)
        
        # Clean up temp directory
        if os.path.exists(self.temp_dir):
            try:
                os.rmdir(self.temp_dir)
            except OSError:
                pass  # Directory not empty, that's ok
    
    def _mock_config_get(self, section, key, fallback=None):
        """Mock configuration getter"""
        config_values = {
            ('MAPPING', 'mapping_file'): self.cache_file,
            ('MAPPING', 'checkpoint_file'): self.checkpoint_file,
            ('IMPORT', 'rate_limit_delay'): '1.5'
        }
        return config_values.get((section, key), fallback or '')
    
    def test_checkpoint_configuration(self):
        """Test that checkpoint configuration is properly loaded"""
        self.assertTrue(self.mapper.checkpoint_enabled)
        self.assertEqual(self.mapper.checkpoint_frequency, 100)
        self.assertTrue(self.mapper.resume_on_restart)
        self.assertEqual(self.mapper.checkpoint_file, self.checkpoint_file)
    
    def test_should_resume_no_checkpoint(self):
        """Test should_resume returns False when no checkpoint exists"""
        self.assertFalse(self.mapper._should_resume())
    
    def test_save_checkpoint_basic(self):
        """Test basic checkpoint saving functionality"""
        processed_coins = ['bitcoin', 'ethereum']
        mapping_data = {'bitcoin': {'exchange': 'kraken'}}
        failed_coins = ['failed_coin']
        start_time = datetime.now()
        
        result = self.mapper._save_checkpoint(
            processed_index=1,
            total_coins=1000,
            processed_coin_ids=processed_coins,
            mapping_data=mapping_data,
            failed_coin_ids=failed_coins,
            start_time=start_time
        )
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.checkpoint_file))
        
        # Verify checkpoint content
        with open(self.checkpoint_file, 'r') as f:
            checkpoint_data = json.load(f)
        
        self.assertEqual(checkpoint_data['status'], 'in_progress')
        self.assertEqual(checkpoint_data['total_coins'], 1000)
        self.assertEqual(checkpoint_data['processed_coins'], 2)
        self.assertEqual(checkpoint_data['last_processed_index'], 1)
        self.assertEqual(checkpoint_data['processed_coin_ids'], processed_coins)
        self.assertEqual(checkpoint_data['failed_coin_ids'], failed_coins)
        self.assertIn('start_time', checkpoint_data)
        self.assertIn('last_checkpoint_time', checkpoint_data)
    
    def test_load_checkpoint_valid(self):
        """Test loading a valid checkpoint"""
        # Create test checkpoint
        checkpoint_data = {
            'status': 'in_progress',
            'total_coins': 1000,
            'processed_coins': 100,
            'last_processed_index': 99,
            'processed_coin_ids': [f'coin_{i}' for i in range(100)],
            'failed_coin_ids': ['failed_coin'],
            'start_time': datetime.now().isoformat(),
            'last_checkpoint_time': datetime.now().isoformat(),
            'batch_size': 50,
            'checkpoint_frequency': 100
        }
        
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)
        
        loaded = self.mapper._load_checkpoint()
        
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded['total_coins'], 1000)
        self.assertEqual(loaded['processed_coins'], 100)
        self.assertEqual(len(loaded['processed_coin_ids']), 100)
    
    def test_load_checkpoint_invalid(self):
        """Test loading an invalid checkpoint"""
        # Create invalid checkpoint (missing required fields)
        invalid_checkpoint = {
            'status': 'in_progress',
            'total_coins': 1000
            # Missing required fields
        }
        
        with open(self.checkpoint_file, 'w') as f:
            json.dump(invalid_checkpoint, f)
        
        loaded = self.mapper._load_checkpoint()
        self.assertIsNone(loaded)
    
    def test_load_checkpoint_expired(self):
        """Test loading an expired checkpoint"""
        # Create expired checkpoint
        old_time = datetime.now() - timedelta(hours=25)  # Older than 24 hours
        checkpoint_data = {
            'status': 'in_progress',
            'total_coins': 1000,
            'processed_coins': 100,
            'last_processed_index': 99,
            'processed_coin_ids': [f'coin_{i}' for i in range(100)],
            'failed_coin_ids': [],
            'start_time': old_time.isoformat(),
            'last_checkpoint_time': old_time.isoformat()
        }
        
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)
        
        loaded = self.mapper._load_checkpoint()
        self.assertIsNone(loaded)
    
    def test_validate_checkpoint_valid(self):
        """Test validating a valid checkpoint"""
        valid_checkpoint = {
            'status': 'in_progress',
            'total_coins': 1000,
            'processed_coins': 100,
            'last_processed_index': 99,
            'processed_coin_ids': [f'coin_{i}' for i in range(100)],
            'failed_coin_ids': [],
            'start_time': datetime.now().isoformat(),
            'last_checkpoint_time': datetime.now().isoformat()
        }
        
        self.assertTrue(self.mapper._validate_checkpoint(valid_checkpoint))
    
    def test_validate_checkpoint_missing_fields(self):
        """Test validating checkpoint with missing required fields"""
        invalid_checkpoint = {
            'status': 'in_progress',
            'total_coins': 1000
            # Missing required fields
        }
        
        self.assertFalse(self.mapper._validate_checkpoint(invalid_checkpoint))
    
    def test_validate_checkpoint_count_mismatch(self):
        """Test validating checkpoint with count mismatch"""
        invalid_checkpoint = {
            'status': 'in_progress',
            'total_coins': 1000,
            'processed_coins': 100,  # Says 100 processed
            'last_processed_index': 99,
            'processed_coin_ids': ['coin_1', 'coin_2'],  # But only 2 IDs
            'failed_coin_ids': [],
            'start_time': datetime.now().isoformat(),
            'last_checkpoint_time': datetime.now().isoformat()
        }
        
        self.assertFalse(self.mapper._validate_checkpoint(invalid_checkpoint))
    
    def test_get_resume_point_no_checkpoint(self):
        """Test getting resume point when no checkpoint exists"""
        all_coins = [{'id': f'coin_{i}'} for i in range(10)]
        
        resume_idx, processed_ids, existing_mapping = self.mapper._get_resume_point(all_coins)
        
        self.assertEqual(resume_idx, 0)
        self.assertEqual(processed_ids, [])
        self.assertEqual(existing_mapping, {})
    
    def test_get_resume_point_with_checkpoint(self):
        """Test getting resume point with existing checkpoint"""
        # Create checkpoint
        checkpoint_data = {
            'status': 'in_progress',
            'total_coins': 10,
            'processed_coins': 5,
            'last_processed_index': 4,
            'processed_coin_ids': [f'coin_{i}' for i in range(5)],
            'failed_coin_ids': [],
            'start_time': datetime.now().isoformat(),
            'last_checkpoint_time': datetime.now().isoformat()
        }
        
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)
        
        # Create partial cache
        cache_data = {
            'mapping': {'coin_0': {'exchange': 'kraken'}},
            'last_update': datetime.now().isoformat(),
            'exchange': 'kraken'
        }
        
        with open(self.cache_file, 'w') as f:
            json.dump(cache_data, f)
        
        all_coins = [{'id': f'coin_{i}'} for i in range(10)]
        
        resume_idx, processed_ids, existing_mapping = self.mapper._get_resume_point(all_coins)
        
        self.assertEqual(resume_idx, 5)  # Should resume from index 5
        self.assertEqual(len(processed_ids), 5)
        self.assertEqual(existing_mapping, {'coin_0': {'exchange': 'kraken'}})
    
    def test_clear_checkpoint(self):
        """Test clearing checkpoint file"""
        # Create checkpoint file
        with open(self.checkpoint_file, 'w') as f:
            json.dump({'test': 'data'}, f)
        
        self.assertTrue(os.path.exists(self.checkpoint_file))
        
        result = self.mapper._clear_checkpoint()
        
        self.assertTrue(result)
        self.assertFalse(os.path.exists(self.checkpoint_file))
    
    def test_update_incremental_cache(self):
        """Test updating the incremental cache"""
        mapping_data = {'bitcoin': {'exchange': 'kraken'}}
        
        result = self.mapper._update_incremental_cache(mapping_data)
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.cache_file))
        
        # Verify cache content
        with open(self.cache_file, 'r') as f:
            cache_data = json.load(f)
        
        self.assertEqual(cache_data['mapping'], mapping_data)
        self.assertEqual(cache_data['exchange'], 'kraken')
        self.assertTrue(cache_data['partial_update'])


class TestKrakenMapperBasic(unittest.TestCase):
    """Test cases for basic KrakenMapper functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create mock configuration
        self.mock_config = Mock(spec=ConfigurationManager)
        self.mock_config.get.return_value = 'test_value'
        self.mock_config.getint.return_value = 100
        self.mock_config.getboolean.return_value = True
        self.mock_config.getfloat.return_value = 1.5
        
        # Create mapper instance
        self.mapper = KrakenMapper(self.mock_config)
    
    def test_get_exchange_name(self):
        """Test that mapper returns correct exchange name"""
        self.assertEqual(self.mapper.get_exchange_name(), "kraken")
    
    @patch('requests.get')
    def test_load_exchange_data_success(self, mock_get):
        """Test successful loading of exchange data"""
        # Mock API responses
        assets_response = Mock()
        assets_response.json.return_value = {
            'error': [],
            'result': {'XXBT': {'altname': 'XBT'}}
        }
        
        pairs_response = Mock()
        pairs_response.json.return_value = {
            'error': [],
            'result': {'XXBTZUSD': {'base': 'XXBT', 'quote': 'ZUSD'}}
        }
        
        mock_get.side_effect = [assets_response, pairs_response]
        
        result = self.mapper.load_exchange_data()
        
        self.assertTrue(result)
        self.assertIn('XXBT', self.mapper.assets)
        self.assertIn('XXBTZUSD', self.mapper.asset_pairs)
    
    @patch('requests.get')
    def test_load_exchange_data_api_error(self, mock_get):
        """Test loading exchange data when API returns error"""
        error_response = Mock()
        error_response.json.return_value = {
            'error': ['API Error'],
            'result': {}
        }
        
        mock_get.return_value = error_response
        
        result = self.mapper.load_exchange_data()
        
        self.assertFalse(result)
    
    def test_map_coin_to_exchange_cached(self):
        """Test mapping coin to exchange using cached data"""
        # Set up cached mapping
        self.mapper.mapping_cache = {
            'bitcoin': {
                'exchange_name': 'kraken',
                'symbol': 'BTCUSD',
                'pair_name': 'XXBTZUSD',
                'base_currency': 'BTC',
                'target_currency': 'USD'
            }
        }
        
        result = self.mapper.map_coin_to_exchange('bitcoin')
        
        self.assertIsNotNone(result)
        self.assertEqual(result.exchange_name, 'kraken')
        self.assertEqual(result.pair_name, 'XXBTZUSD')
    
    def test_map_coin_to_exchange_not_found(self):
        """Test mapping coin to exchange when coin not found"""
        self.mapper.mapping_cache = {}
        
        result = self.mapper.map_coin_to_exchange('unknown_coin')
        
        self.assertIsNone(result)
    
    def test_is_tradeable_true(self):
        """Test is_tradeable returns True for cached coin"""
        self.mapper.mapping_cache = {'bitcoin': {'exchange': 'kraken'}}
        
        result = self.mapper.is_tradeable('bitcoin')
        
        self.assertTrue(result)
    
    def test_is_tradeable_false(self):
        """Test is_tradeable returns False for non-cached coin"""
        self.mapper.mapping_cache = {}
        
        result = self.mapper.is_tradeable('unknown_coin')
        
        self.assertFalse(result)
    
    def test_get_symbol_mapping_found(self):
        """Test getting symbol mapping when mapping exists"""
        self.mapper.mapping_cache = {
            'bitcoin': {
                'exchange_name': 'kraken',
                'symbol': 'BTCUSD',
                'pair_name': 'XXBTZUSD',
                'base_currency': 'BTC',
                'target_currency': 'USD'
            }
        }
        
        result = self.mapper.get_symbol_mapping('bitcoin')
        
        self.assertEqual(result, 'XXBTZUSD')
    
    def test_get_symbol_mapping_not_found(self):
        """Test getting symbol mapping when mapping doesn't exist"""
        self.mapper.mapping_cache = {}
        
        result = self.mapper.get_symbol_mapping('unknown_coin')
        
        self.assertIsNone(result)


class TestKrakenMapperIntegration(unittest.TestCase):
    """Integration tests for KrakenMapper with checkpoint/resume"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create mock configuration
        self.mock_config = Mock(spec=ConfigurationManager)
        self.mock_config.get.side_effect = self._mock_config_get
        self.mock_config.getint.return_value = 2  # Small checkpoint frequency for testing
        self.mock_config.getboolean.return_value = True
        self.mock_config.getfloat.return_value = 0.1  # Fast rate limiting for tests
        
        self.checkpoint_file = os.path.join(self.temp_dir, "integration_checkpoint.json")
        self.cache_file = os.path.join(self.temp_dir, "integration_cache.json")
        
        # Create mapper instance
        self.mapper = KrakenMapper(self.mock_config)
        self.mapper.checkpoint_file = self.checkpoint_file
        self.mapper.cache_file = self.cache_file
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Clean up temp files
        for file in os.listdir(self.temp_dir):
            file_path = os.path.join(self.temp_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        
        try:
            os.rmdir(self.temp_dir)
        except OSError:
            pass
    
    def _mock_config_get(self, section, key, fallback=None):
        """Mock configuration getter"""
        config_values = {
            ('MAPPING', 'mapping_file'): self.cache_file,
            ('MAPPING', 'checkpoint_file'): self.checkpoint_file,
            ('IMPORT', 'rate_limit_delay'): '0.1'
        }
        return config_values.get((section, key), fallback or '')
    
    @patch.object(KrakenMapper, 'load_exchange_data')
    @patch.object(KrakenMapper, '_extract_kraken_info')
    def test_build_coin_mapping_with_checkpoints(self, mock_extract, mock_load_exchange):
        """Test building coin mapping with checkpoint saves"""
        # Mock successful exchange data loading
        mock_load_exchange.return_value = True
        
        # Mock data provider
        mock_provider = Mock()
        mock_provider.get_all_coins.return_value = [
            {'id': 'bitcoin', 'name': 'Bitcoin'},
            {'id': 'ethereum', 'name': 'Ethereum'},
            {'id': 'cardano', 'name': 'Cardano'},
            {'id': 'polkadot', 'name': 'Polkadot'}
        ]
        
        def mock_get_exchange_data(coin_id):
            # Return mock exchange data for some coins
            if coin_id in ['bitcoin', 'ethereum']:
                return {'tickers': [{'base': coin_id.upper()[:3], 'target': 'USD'}]}
            return None
        
        mock_provider.get_exchange_data = mock_get_exchange_data
        
        # Mock kraken info extraction
        def mock_extract_side_effect(exchange_data):
            if exchange_data:
                return {
                    'exchange_name': 'kraken',
                    'symbol': 'MOCK',
                    'pair_name': 'MOCKUSD',
                    'base_currency': 'MOCK',
                    'target_currency': 'USD'
                }
            return None
        
        mock_extract.side_effect = mock_extract_side_effect
        
        # Run the mapping build
        with patch('time.sleep'):  # Skip actual sleep
            result = self.mapper._build_coin_mapping(mock_provider)
        
        # Verify results
        self.assertIsInstance(result, dict)
        # Should have created mappings for bitcoin and ethereum
        self.assertEqual(len(result), 2)
        
        # Verify checkpoint was cleaned up (since process completed)
        self.assertFalse(os.path.exists(self.checkpoint_file))
    
    @patch.object(KrakenMapper, 'load_exchange_data')
    @patch.object(KrakenMapper, '_extract_kraken_info')
    def test_build_coin_mapping_resume_from_checkpoint(self, mock_extract, mock_load_exchange):
        """Test resuming coin mapping from checkpoint"""
        # Create existing checkpoint
        checkpoint_data = {
            'status': 'in_progress',
            'total_coins': 4,
            'processed_coins': 2,
            'last_processed_index': 1,
            'processed_coin_ids': ['bitcoin', 'ethereum'],
            'failed_coin_ids': [],
            'start_time': datetime.now().isoformat(),
            'last_checkpoint_time': datetime.now().isoformat(),
            'batch_size': 50,
            'checkpoint_frequency': 2
        }
        
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)
        
        # Create existing partial cache
        cache_data = {
            'mapping': {
                'bitcoin': {
                    'exchange_name': 'kraken',
                    'symbol': 'BTCUSD',
                    'pair_name': 'XXBTZUSD'
                }
            },
            'last_update': datetime.now().isoformat(),
            'exchange': 'kraken',
            'partial_update': True
        }
        
        with open(self.cache_file, 'w') as f:
            json.dump(cache_data, f)
        
        # Mock successful exchange data loading
        mock_load_exchange.return_value = True
        
        # Mock data provider
        mock_provider = Mock()
        mock_provider.get_all_coins.return_value = [
            {'id': 'bitcoin', 'name': 'Bitcoin'},      # Already processed
            {'id': 'ethereum', 'name': 'Ethereum'},    # Already processed
            {'id': 'cardano', 'name': 'Cardano'},      # Will be processed
            {'id': 'polkadot', 'name': 'Polkadot'}     # Will be processed
        ]
        
        def mock_get_exchange_data(coin_id):
            if coin_id in ['cardano', 'polkadot']:
                return {'tickers': [{'base': coin_id.upper()[:3], 'target': 'USD'}]}
            return None
        
        mock_provider.get_exchange_data = mock_get_exchange_data
        
        # Mock kraken info extraction
        def mock_extract_side_effect(exchange_data):
            if exchange_data:
                return {
                    'exchange_name': 'kraken',
                    'symbol': 'MOCK',
                    'pair_name': 'MOCKUSD',
                    'base_currency': 'MOCK',
                    'target_currency': 'USD'
                }
            return None
        
        mock_extract.side_effect = mock_extract_side_effect
        
        # Run the mapping build (should resume)
        with patch('time.sleep'):  # Skip actual sleep
            result = self.mapper._build_coin_mapping(mock_provider)
        
        # Verify results
        self.assertIsInstance(result, dict)
        # Should have mappings for bitcoin (from cache) + cardano, polkadot (newly processed)
        self.assertEqual(len(result), 3)
        self.assertIn('bitcoin', result)  # From existing cache
        self.assertIn('cardano', result)   # Newly processed
        self.assertIn('polkadot', result)  # Newly processed
        
        # Verify checkpoint was cleaned up after completion
        self.assertFalse(os.path.exists(self.checkpoint_file))
    
    @patch.object(KrakenMapper, 'load_exchange_data')
    def test_build_coin_mapping_interrupt_handling(self, mock_load_exchange):
        """Test handling of KeyboardInterrupt during mapping build"""
        # Mock successful exchange data loading
        mock_load_exchange.return_value = True
        
        # Mock data provider
        mock_provider = Mock()
        mock_provider.get_all_coins.return_value = [
            {'id': 'bitcoin', 'name': 'Bitcoin'},
            {'id': 'ethereum', 'name': 'Ethereum'},
            {'id': 'cardano', 'name': 'Cardano'}
        ]
        
        # Mock get_exchange_data to raise KeyboardInterrupt after first coin
        call_count = 0
        def mock_get_exchange_data(coin_id):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise KeyboardInterrupt("Simulated interrupt")
            return {'tickers': [{'base': coin_id.upper()[:3], 'target': 'USD'}]}
        
        mock_provider.get_exchange_data = mock_get_exchange_data
        
        # Run the mapping build (should be interrupted)
        with patch('time.sleep'):  # Skip actual sleep
            with self.assertRaises(KeyboardInterrupt):
                self.mapper._build_coin_mapping(mock_provider)
        
        # Verify checkpoint was saved before interrupt
        self.assertTrue(os.path.exists(self.checkpoint_file))
        
        with open(self.checkpoint_file, 'r') as f:
            checkpoint_data = json.load(f)
        
        self.assertEqual(checkpoint_data['status'], 'in_progress')
        self.assertGreater(len(checkpoint_data['processed_coin_ids']), 0)


if __name__ == '__main__':
    unittest.main()