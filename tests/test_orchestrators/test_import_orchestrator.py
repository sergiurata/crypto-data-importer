"""
Test cases for ImportOrchestrator
"""
import sys
import unittest
import tempfile
import os
from pathlib import Path

import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Add src to path
src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from orchestrators.import_orchestrator import ImportOrchestrator, ImportResult, ProcessResult
from core.configuration_manager import ConfigurationManager
from providers.abstract_data_provider import AbstractDataProvider
from mappers.abstract_exchange_mapper import AbstractExchangeMapper, ExchangeInfo
from adapters.abstract_database_adapter import AbstractDatabaseAdapter
from filters.data_filter import DataFilter
from schedulers.update_scheduler import UpdateScheduler


class TestImportResult(unittest.TestCase):
    """Test cases for ImportResult dataclass"""
    
    def test_import_result_creation(self):
        """Test creating an ImportResult"""
        result = ImportResult(
            total_processed=100,
            new_records=500,
            updated_records=50,
            failed_count=5,
            kraken_count=30,
            execution_time=120.5
        )
        
        self.assertEqual(result.total_processed, 100)
        self.assertEqual(result.new_records, 500)
        self.assertEqual(result.updated_records, 50)
        self.assertEqual(result.failed_count, 5)
        self.assertEqual(result.kraken_count, 30)
        self.assertEqual(result.execution_time, 120.5)
        self.assertEqual(result.errors, [])  # Default empty list
    
    def test_import_result_with_errors(self):
        """Test ImportResult with errors"""
        errors = ["Error 1", "Error 2"]
        result = ImportResult(errors=errors)
        
        self.assertEqual(result.errors, errors)


class TestProcessResult(unittest.TestCase):
    """Test cases for ProcessResult dataclass"""
    
    def test_process_result_success(self):
        """Test creating successful ProcessResult"""
        result = ProcessResult(
            success=True,
            new_records=10,
            updated_records=2,
            is_kraken=True
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.new_records, 10)
        self.assertEqual(result.updated_records, 2)
        self.assertTrue(result.is_kraken)
        self.assertEqual(result.error_message, "")
    
    def test_process_result_failure(self):
        """Test creating failed ProcessResult"""
        result = ProcessResult(
            success=False,
            error_message="Processing failed"
        )
        
        self.assertFalse(result.success)
        self.assertEqual(result.error_message, "Processing failed")
        self.assertEqual(result.new_records, 0)
        self.assertEqual(result.updated_records, 0)


class TestImportOrchestrator(unittest.TestCase):
    """Test cases for ImportOrchestrator class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create temporary config file
        self.temp_dir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.temp_dir, "test_config.ini")
        
        # Create mock components
        self.mock_data_provider = Mock(spec=AbstractDataProvider)
        self.mock_exchange_mapper = Mock(spec=AbstractExchangeMapper)
        self.mock_database_adapter = Mock(spec=AbstractDatabaseAdapter)
        
        # Setup mock returns
        self.mock_data_provider.get_all_coins.return_value = self._get_test_coins()
        self.mock_data_provider.get_market_data.return_value = self._get_test_market_data()
        # Add format_market_data method to mock (it exists in CoinGeckoProvider)
        self.mock_data_provider.format_market_data = Mock(return_value=self._get_test_dataframe())
        
        self.mock_exchange_mapper.get_exchange_name.return_value = "kraken"
        self.mock_exchange_mapper.is_tradeable.return_value = False
        self.mock_exchange_mapper.map_coin_to_exchange.return_value = None
        self.mock_exchange_mapper.build_mapping.return_value = True
        self.mock_exchange_mapper.validate_mapping.return_value = True
        
        self.mock_database_adapter.validate_connection.return_value = True
        self.mock_database_adapter.create_groups.return_value = True
        self.mock_database_adapter.import_data.return_value = True
        self.mock_database_adapter.update_data.return_value = (10, 2)
        
        # Create orchestrator
        self.orchestrator = ImportOrchestrator(self.config_path)
    
    def tearDown(self):
        """Clean up test fixtures"""
        if os.path.exists(self.config_path):
            os.remove(self.config_path)
        os.rmdir(self.temp_dir)
    
    def _get_test_coins(self):
        """Get test coin data"""
        return [
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"},
            {"id": "ethereum", "symbol": "eth", "name": "Ethereum"},
            {"id": "cardano", "symbol": "ada", "name": "Cardano"}
        ]
    
    def _get_test_market_data(self):
        """Get test market data"""
        return {
            "prices": [[1609459200000, 29000.0], [1609545600000, 30000.0]],
            "market_caps": [[1609459200000, 540000000000], [1609545600000, 560000000000]],
            "total_volumes": [[1609459200000, 50000000000], [1609545600000, 52000000000]]
        }
    
    def _get_test_dataframe(self):
        """Get test DataFrame"""
        return pd.DataFrame({
            'Open': [29000.0, 30000.0],
            'High': [29500.0, 30500.0],
            'Low': [28500.0, 29500.0],
            'Close': [29000.0, 30000.0],
            'Volume': [50000000000, 52000000000],
            'MarketCap': [540000000000, 560000000000]
        }, index=pd.date_range('2021-01-01', periods=2, freq='D'))
    
    @patch('orchestrators.import_orchestrator.ConfigurationManager')
    @patch('orchestrators.import_orchestrator.LoggingManager')
    def test_init(self, mock_logging_manager, mock_config_manager):
        """Test orchestrator initialization"""
        orchestrator = ImportOrchestrator("test_config.ini")
        
        self.assertIsNotNone(orchestrator.config)
        self.assertIsNotNone(orchestrator.logging_manager)
        self.assertFalse(orchestrator.is_initialized)
        self.assertIsNone(orchestrator.last_import_result)
    
    def test_initialize_success(self):
        """Test successful initialization"""
        result = self.orchestrator.initialize(
            self.mock_data_provider,
            [self.mock_exchange_mapper],
            self.mock_database_adapter
        )
        
        self.assertTrue(result)
        self.assertTrue(self.orchestrator.is_initialized)
        self.assertEqual(self.orchestrator.data_provider, self.mock_data_provider)
        self.assertEqual(self.orchestrator.exchange_mappers, [self.mock_exchange_mapper])
        self.assertEqual(self.orchestrator.database_adapter, self.mock_database_adapter)
    
    def test_initialize_validation_failure(self):
        """Test initialization with validation failure"""
        # Make database validation fail
        self.mock_database_adapter.validate_connection.return_value = False
        
        result = self.orchestrator.initialize(
            self.mock_data_provider,
            [self.mock_exchange_mapper],
            self.mock_database_adapter
        )
        
        self.assertFalse(result)
        self.assertFalse(self.orchestrator.is_initialized)
    
    def test_validate_setup_success(self):
        """Test successful setup validation"""
        self.orchestrator.data_provider = self.mock_data_provider
        self.orchestrator.exchange_mappers = [self.mock_exchange_mapper]
        self.orchestrator.database_adapter = self.mock_database_adapter
        
        # Mock configuration values
        with patch.object(self.orchestrator.config, 'get') as mock_get:
            mock_get.return_value = "test_value"
            result = self.orchestrator.validate_setup()
        
        self.assertTrue(result)
    
    def test_validate_setup_missing_data_provider(self):
        """Test setup validation with missing data provider"""
        self.orchestrator.data_provider = None
        self.orchestrator.exchange_mappers = [self.mock_exchange_mapper]
        self.orchestrator.database_adapter = self.mock_database_adapter
        
        result = self.orchestrator.validate_setup()
        
        self.assertFalse(result)
    
    def test_validate_setup_missing_database_adapter(self):
        """Test setup validation with missing database adapter"""
        self.orchestrator.data_provider = self.mock_data_provider
        self.orchestrator.exchange_mappers = [self.mock_exchange_mapper]
        self.orchestrator.database_adapter = None
        
        result = self.orchestrator.validate_setup()
        
        self.assertFalse(result)
    
    def test_run_import_not_initialized(self):
        """Test running import when not initialized"""
        result = self.orchestrator.run_import()
        
        self.assertIsInstance(result, ImportResult)
        self.assertIn("Orchestrator not initialized", result.errors)
    
    @patch('orchestrators.import_orchestrator.time.time')
    def test_run_import_success(self, mock_time):
        """Test successful import run"""
        # Setup time mocking - start time 0, end time 120, many intermediate values
        def time_generator():
            # First few calls for initialization
            for i in range(10):
                yield i
            # Then many 120s for the final time calls
            while True:
                yield 120
        
        mock_time.side_effect = time_generator()
        
        # Initialize orchestrator
        self.orchestrator.initialize(
            self.mock_data_provider,
            [self.mock_exchange_mapper],
            self.mock_database_adapter
        )
        
        # Mock configuration values
        with patch.object(self.orchestrator.config, 'getint') as mock_getint, \
             patch.object(self.orchestrator.config, 'getfloat') as mock_getfloat, \
             patch.object(self.orchestrator.config, 'getboolean') as mock_getboolean:
            
            mock_getint.return_value = 10  # max_coins
            mock_getfloat.return_value = 0  # min_market_cap
            mock_getboolean.return_value = False  # force_full_update
            
            # Mock data filter
            mock_filter = Mock()
            mock_filter.apply_filters.return_value = True
            self.orchestrator.data_filter = mock_filter
            
            # Mock update scheduler
            mock_scheduler = Mock()
            self.orchestrator.update_scheduler = mock_scheduler
            
            result = self.orchestrator.run_import()
        
        self.assertIsInstance(result, ImportResult)
        self.assertEqual(result.execution_time, 117)
        self.assertEqual(result.total_processed, 3)  # All test coins processed
    
    def test_run_import_no_coins(self):
        """Test import run when no coins are retrieved"""
        # Mock empty coins list
        self.mock_data_provider.get_all_coins.return_value = []
        
        # Initialize orchestrator
        self.orchestrator.initialize(
            self.mock_data_provider,
            [self.mock_exchange_mapper],
            self.mock_database_adapter
        )
        
        result = self.orchestrator.run_import()
        
        self.assertIsInstance(result, ImportResult)
        self.assertIn("No coins retrieved from data provider", result.errors)
    
    def test_process_coin_success(self):
        """Test successful coin processing"""
        coin = {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"}
        
        # Initialize orchestrator
        self.orchestrator.initialize(
            self.mock_data_provider,
            [self.mock_exchange_mapper],
            self.mock_database_adapter
        )
        
        result = self.orchestrator.process_coin(coin)
        
        self.assertIsInstance(result, ProcessResult)
        self.assertTrue(result.success)
    
    def test_process_coin_no_market_data(self):
        """Test coin processing when no market data available"""
        coin = {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"}
        
        # Mock no market data
        self.mock_data_provider.get_market_data.return_value = None
        
        # Initialize orchestrator
        self.orchestrator.initialize(
            self.mock_data_provider,
            [self.mock_exchange_mapper],
            self.mock_database_adapter
        )
        
        result = self.orchestrator.process_coin(coin)
        
        self.assertIsInstance(result, ProcessResult)
        self.assertFalse(result.success)
        self.assertIn("No market data available", result.error_message)
    
    def test_process_coin_no_formatted_data(self):
        """Test coin processing when data formatting fails"""
        coin = {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"}
        
        # Mock format_market_data to return None
        self.mock_data_provider.format_market_data.return_value = None
        
        # Initialize orchestrator
        self.orchestrator.initialize(
            self.mock_data_provider,
            [self.mock_exchange_mapper],
            self.mock_database_adapter
        )
        
        result = self.orchestrator.process_coin(coin)
        
        self.assertIsInstance(result, ProcessResult)
        self.assertFalse(result.success)
        self.assertIn("No formatted data available", result.error_message)
    
    def test_apply_exchange_mapping_no_exchanges(self):
        """Test exchange mapping when coin not on any exchange"""
        coin = {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"}
        
        # Initialize orchestrator
        self.orchestrator.initialize(
            self.mock_data_provider,
            [self.mock_exchange_mapper],
            self.mock_database_adapter
        )
        
        result = self.orchestrator.apply_exchange_mapping(coin)
        
        self.assertEqual(result['id'], "bitcoin")
        self.assertEqual(result['exchanges'], {})
        self.assertNotIn('is_kraken', result)
    
    def test_apply_exchange_mapping_with_kraken(self):
        """Test exchange mapping when coin is on Kraken"""
        coin = {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"}
        
        # Mock Kraken exchange info
        kraken_info = ExchangeInfo(
            exchange_name="kraken",
            symbol="BTCUSD",
            pair_name="XXBTZUSD",
            base_currency="BTC",
            target_currency="USD"
        )
        
        self.mock_exchange_mapper.is_tradeable.return_value = True
        self.mock_exchange_mapper.map_coin_to_exchange.return_value = kraken_info
        
        # Initialize orchestrator
        self.orchestrator.initialize(
            self.mock_data_provider,
            [self.mock_exchange_mapper],
            self.mock_database_adapter
        )
        
        result = self.orchestrator.apply_exchange_mapping(coin)
        
        self.assertEqual(result['id'], "bitcoin")
        self.assertTrue(result['is_kraken'])
        self.assertEqual(result['kraken_symbol'], "BTCUSD")
        self.assertEqual(result['kraken_pair_name'], "XXBTZUSD")
        self.assertIn('kraken', result['exchanges'])
    
    def test_import_coin_data_regular_coin(self):
        """Test importing data for regular coin"""
        coin = {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "exchanges": {}
        }
        
        data = self._get_test_dataframe()
        
        # Initialize orchestrator
        self.orchestrator.initialize(
            self.mock_data_provider,
            [self.mock_exchange_mapper],
            self.mock_database_adapter
        )
        
        # Mock symbol doesn't exist
        self.mock_database_adapter.symbol_exists.return_value = False
        
        result = self.orchestrator.import_coin_data(coin, data)
        
        self.assertTrue(result)
        self.mock_database_adapter.import_data.assert_called_once()
    
    def test_import_coin_data_kraken_coin(self):
        """Test importing data for Kraken coin"""
        coin = {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "is_kraken": True,
            "kraken_pair_name": "XXBTZUSD",
            "exchanges": {
                "kraken": {
                    "symbol": "BTCUSD",
                    "pair_name": "XXBTZUSD",
                    "base_currency": "BTC",
                    "target_currency": "USD"
                }
            }
        }
        
        data = self._get_test_dataframe()
        
        # Initialize orchestrator
        self.orchestrator.initialize(
            self.mock_data_provider,
            [self.mock_exchange_mapper],
            self.mock_database_adapter
        )
        
        # Mock symbol doesn't exist
        self.mock_database_adapter.symbol_exists.return_value = False
        
        result = self.orchestrator.import_coin_data(coin, data)
        
        self.assertTrue(result)
        # Should use Kraken pair name as symbol
        call_args = self.mock_database_adapter.import_data.call_args
        self.assertEqual(call_args[0][0], "XXBTZUSD")  # symbol parameter
    
    def test_get_display_name_regular_coin(self):
        """Test getting display name for regular coin"""
        coin = {"symbol": "btc", "name": "Bitcoin"}
        
        result = self.orchestrator._get_display_name(coin)
        
        self.assertEqual(result, "btc - Bitcoin")
    
    def test_get_display_name_kraken_coin(self):
        """Test getting display name for Kraken coin"""
        coin = {
            "symbol": "btc",
            "name": "Bitcoin",
            "is_kraken": True,
            "kraken_pair_name": "XXBTZUSD"
        }
        
        result = self.orchestrator._get_display_name(coin)
        
        self.assertEqual(result, "XXBTZUSD - Bitcoin")
    
    def test_run_update_not_initialized(self):
        """Test running update when not initialized"""
        result = self.orchestrator.run_update()
        
        self.assertFalse(result)
    
    def test_run_update_success(self):
        """Test successful update run"""
        # Initialize orchestrator
        self.orchestrator.initialize(
            self.mock_data_provider,
            [self.mock_exchange_mapper],
            self.mock_database_adapter
        )
        
        # Mock symbols list
        self.mock_database_adapter.get_symbol_list.return_value = ["BTC", "ETH"]
        self.mock_database_adapter.get_symbol_metadata.return_value = {"CoinGeckoID": "bitcoin"}
        
        # Mock configuration
        with patch.object(self.orchestrator.config, 'getint') as mock_getint, \
             patch.object(self.orchestrator.config, 'getfloat') as mock_getfloat:
            
            mock_getint.return_value = 7  # days_back
            mock_getfloat.return_value = 1.5  # rate_limit_delay
            
            result = self.orchestrator.run_update()
        
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()