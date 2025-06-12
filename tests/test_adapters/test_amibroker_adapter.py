"""
Test cases for AmiBrokerAdapter
"""

import unittest
import tempfile
import os
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from src.adapters.amibroker_adapter import AmiBrokerAdapter
from src.core.configuration_manager import ConfigurationManager


class TestAmiBrokerAdapter(unittest.TestCase):
    """Test cases for AmiBrokerAdapter class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create mock configuration
        self.mock_config = Mock(spec=ConfigurationManager)
        self.mock_config.get.return_value = ""
        
        # Create test data
        self.test_data = pd.DataFrame({
            'Open': [100.0, 101.0, 102.0],
            'High': [105.0, 106.0, 107.0],
            'Low': [95.0, 96.0, 97.0],
            'Close': [104.0, 105.0, 106.0],
            'Volume': [1000000, 1100000, 1200000],
            'MarketCap': [1e9, 1.1e9, 1.2e9]
        }, index=pd.date_range('2023-01-01', periods=3, freq='D'))
        
        # Create temporary database path
        self.temp_dir = tempfile.mkdtemp()
        self.test_db_path = os.path.join(self.temp_dir, "test.adb")
    
    def tearDown(self):
        """Clean up test fixtures"""
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
        os.rmdir(self.temp_dir)
    
    @patch('win32com.client.Dispatch')
    def test_get_existing_range_no_data(self, mock_dispatch):
        """Test getting existing data range when no data exists"""
        # Setup mock with no quotations
        mock_com = Mock()
        mock_stock = Mock()
        mock_quotations = Mock()
        mock_quotations.Count = 0
        
        mock_stock.Quotations = mock_quotations
        mock_com.Stocks.return_value = mock_stock
        mock_dispatch.return_value = mock_com
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        start_date, end_date = adapter.get_existing_range('TEST')
        
        self.assertIsNone(start_date)
        self.assertIsNone(end_date)
    
    @patch('win32com.client.Dispatch')
    def test_update_data_new_symbol(self, mock_dispatch):
        """Test updating data for a new symbol (no existing data)"""
        mock_com = Mock()
        mock_stock = Mock()
        mock_quotations = Mock()
        mock_quote = Mock()
        
        mock_com.Stocks.return_value = mock_stock
        mock_stock.Quotations = mock_quotations
        mock_quotations.Add.return_value = mock_quote
        mock_dispatch.return_value = mock_com
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        # Mock get_existing_range to return None (no existing data)
        with patch.object(adapter, 'get_existing_range', return_value=(None, None)):
            new_records, updated_records = adapter.update_data('TEST', self.test_data)
        
        self.assertEqual(new_records, len(self.test_data))
        self.assertEqual(updated_records, 0)
        self.assertEqual(mock_quotations.Add.call_count, len(self.test_data))
    
    @patch('win32com.client.Dispatch')
    def test_update_data_existing_symbol(self, mock_dispatch):
        """Test updating data for an existing symbol"""
        mock_com = Mock()
        mock_stock = Mock()
        mock_quotations = Mock()
        mock_quote = Mock()
        
        mock_com.Stocks.return_value = mock_stock
        mock_stock.Quotations = mock_quotations
        mock_quotations.Add.return_value = mock_quote
        mock_dispatch.return_value = mock_com
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        # Mock existing data range
        existing_start = datetime(2023, 1, 2)
        existing_end = datetime(2023, 1, 2)
        
        with patch.object(adapter, 'get_existing_range', return_value=(existing_start, existing_end)):
            with patch.object(adapter, '_update_existing_quotation', return_value=False):
                new_records, updated_records = adapter.update_data('TEST', self.test_data)
        
        # Should add records outside existing range
        self.assertGreater(new_records, 0)
        self.assertEqual(updated_records, 0)
    
    @patch('win32com.client.Dispatch')
    def test_create_groups_success(self, mock_dispatch):
        """Test successful creation of AmiBroker groups"""
        mock_com = Mock()
        mock_groups = Mock()
        mock_kraken_group = Mock()
        mock_other_group = Mock()
        
        mock_groups.side_effect = lambda id: mock_kraken_group if id == 253 else mock_other_group
        mock_com.Groups = mock_groups
        mock_dispatch.return_value = mock_com
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        result = adapter.create_groups()
        
        self.assertTrue(result)
        mock_kraken_group.Name = "Crypto - Kraken Tradeable"
        mock_other_group.Name = "Crypto - Other Exchanges"
    
    @patch('win32com.client.Dispatch')
    def test_get_symbol_list(self, mock_dispatch):
        """Test getting list of symbols from database"""
        mock_com = Mock()
        mock_stocks = Mock()
        mock_stocks.Count = 2
        
        mock_stock1 = Mock()
        mock_stock1.Ticker = "BTC"
        mock_stock2 = Mock()
        mock_stock2.Ticker = "ETH"
        
        mock_stocks.side_effect = lambda i: mock_stock1 if i == 0 else mock_stock2
        mock_com.Stocks = mock_stocks
        mock_dispatch.return_value = mock_com
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        symbols = adapter.get_symbol_list()
        
        self.assertEqual(len(symbols), 2)
        self.assertIn("BTC", symbols)
        self.assertIn("ETH", symbols)
    
    @patch('win32com.client.Dispatch')
    def test_delete_symbol_success(self, mock_dispatch):
        """Test successful symbol deletion"""
        mock_com = Mock()
        mock_stock = Mock()
        mock_quotations = Mock()
        
        mock_stock.Quotations = mock_quotations
        mock_com.Stocks.return_value = mock_stock
        mock_dispatch.return_value = mock_com
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        result = adapter.delete_symbol('TEST')
        
        self.assertTrue(result)
        mock_quotations.Clear.assert_called_once()
        mock_stock.Save.assert_called_once()
    
    @patch('win32com.client.Dispatch')
    def test_get_symbol_metadata(self, mock_dispatch):
        """Test getting symbol metadata"""
        mock_com = Mock()
        mock_stock = Mock()
        mock_stock.FullName = "Test Coin"
        mock_stock.GroupID = 253
        mock_stock.MarketID = 1
        mock_stock.GetExtraData.side_effect = lambda key: {
            'CoinGeckoID': 'test-coin',
            'Kraken': '1'
        }.get(key, '')
        
        mock_com.Stocks.return_value = mock_stock
        mock_dispatch.return_value = mock_com
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        metadata = adapter.get_symbol_metadata('TEST')
        
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata['symbol'], 'TEST')
        self.assertEqual(metadata['full_name'], 'Test Coin')
        self.assertEqual(metadata['group_id'], 253)
        self.assertEqual(metadata['CoinGeckoID'], 'test-coin')
    
    @patch('win32com.client.Dispatch')
    def test_set_symbol_metadata(self, mock_dispatch):
        """Test setting symbol metadata"""
        mock_com = Mock()
        mock_stock = Mock()
        mock_com.Stocks.return_value = mock_stock
        mock_dispatch.return_value = mock_com
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        metadata = {
            'full_name': 'Test Coin',
            'group_id': 253,
            'market_id': 1,
            'CoinGeckoID': 'test-coin'
        }
        
        result = adapter.set_symbol_metadata('TEST', metadata)
        
        # Since base class returns False, this should return False
        self.assertFalse(result)
    
    def test_add_quotation(self):
        """Test adding a single quotation"""
        adapter = AmiBrokerAdapter(self.mock_config)
        
        mock_quotations = Mock()
        mock_quote = Mock()
        mock_quotations.Add.return_value = mock_quote
        
        test_date = self.test_data.index[0]
        test_row = self.test_data.iloc[0]
        
        adapter._add_quotation(mock_quotations, test_date, test_row)
        
        mock_quotations.Add.assert_called_once()
        self.assertEqual(mock_quote.Open, float(test_row['Open']))
        self.assertEqual(mock_quote.High, float(test_row['High']))
        self.assertEqual(mock_quote.Low, float(test_row['Low']))
        self.assertEqual(mock_quote.Close, float(test_row['Close']))
        self.assertEqual(mock_quote.Volume, float(test_row['Volume']))
    
    def test_update_existing_quotation_changed(self):
        """Test updating existing quotation when values have changed"""
        adapter = AmiBrokerAdapter(self.mock_config)
        
        # Setup mock quotations
        mock_quotations = Mock()
        mock_quotations.Count = 1
        
        mock_quote = Mock()
        mock_quote.Date.year = 2023
        mock_quote.Date.month = 1
        mock_quote.Date.day = 1
        mock_quote.Close = 100.0  # Different from test data
        mock_quote.Volume = 500000  # Different from test data
        
        mock_quotations.side_effect = lambda i: mock_quote
        
        test_date = self.test_data.index[0]
        test_row = self.test_data.iloc[0]
        
        result = adapter._update_existing_quotation(mock_quotations, test_date, test_row)
        
        self.assertTrue(result)
        self.assertEqual(mock_quote.Close, float(test_row['Close']))
        self.assertEqual(mock_quote.Volume, float(test_row['Volume']))
    
    def test_update_existing_quotation_unchanged(self):
        """Test updating existing quotation when values haven't changed"""
        adapter = AmiBrokerAdapter(self.mock_config)
        
        # Setup mock quotations
        mock_quotations = Mock()
        mock_quotations.Count = 1
        
        mock_quote = Mock()
        mock_quote.Date.year = 2023
        mock_quote.Date.month = 1
        mock_quote.Date.day = 1
        mock_quote.Close = 100.0  # Same as test data
        mock_quote.Volume = 1000000  # Same as test data
        
        mock_quotations.side_effect = lambda i: mock_quote
        
        test_date = self.test_data.index[0]
        test_row = self.test_data.iloc[0]
        
        result = adapter._update_existing_quotation(mock_quotations, test_date, test_row)
        
        self.assertFalse(result)
    
    def test_set_stock_metadata(self):
        """Test setting stock metadata"""
        adapter = AmiBrokerAdapter(self.mock_config)
        
        mock_stock = Mock()
        
        metadata = {
            'full_name': 'Test Coin',
            'group_id': 253,
            'market_id': 1,
            'CoinGeckoID': 'test-coin',
            'Kraken': '1'
        }
        
        adapter._set_stock_metadata(mock_stock, metadata)
        
        self.assertEqual(mock_stock.FullName, 'Test Coin')
        self.assertEqual(mock_stock.GroupID, 253)
        self.assertEqual(mock_stock.MarketID, 1)
        mock_stock.SetExtraData.assert_called()
    
    @patch('win32com.client.Dispatch')
    def test_get_current_database_success(self, mock_dispatch):
        """Test getting current database path successfully"""
        mock_com = Mock()
        mock_com.DatabasePath = "/path/to/database.adb"
        mock_dispatch.return_value = mock_com
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        result = adapter._get_current_database()
        
        self.assertEqual(result, "/path/to/database.adb")
    
    @patch('win32com.client.Dispatch')
    def test_get_current_database_no_path(self, mock_dispatch):
        """Test getting current database when no path available"""
        mock_com = Mock()
        del mock_com.DatabasePath  # Simulate missing attribute
        mock_dispatch.return_value = mock_com
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        result = adapter._get_current_database()
        
        self.assertIsNone(result)
    
    def test_get_database_stats(self):
        """Test getting database statistics"""
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.database_path = "/test/path.adb"
        adapter.connection_verified = True
        
        with patch.object(adapter, 'get_symbol_list', return_value=['BTC', 'ETH']):
            with patch.object(adapter, 'get_existing_range', return_value=(datetime(2023, 1, 1), datetime(2023, 1, 31))):
                stats = adapter.get_database_stats()
        
        self.assertEqual(stats['total_symbols'], 2)
        self.assertEqual(stats['database_path'], "/test/path.adb")
        self.assertTrue(stats['connection_status'])
        self.assertIn('sample_date_range', stats)


class TestAmiBrokerAdapterValidation(unittest.TestCase):
    """Additional validation tests for AmiBrokerAdapter"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_config = Mock(spec=ConfigurationManager)
        self.adapter = AmiBrokerAdapter(self.mock_config)
    
    def test_validate_connection_default(self):
        """Test default connection validation"""
        self.adapter.connection_verified = True
        result = self.adapter.validate_connection()
        self.assertTrue(result)
        
        self.adapter.connection_verified = False
        result = self.adapter.validate_connection()
        self.assertFalse(result)
    
    def test_symbol_exists_true(self):
        """Test symbol_exists when symbol exists"""
        with patch.object(self.adapter, 'get_symbol_list', return_value=['BTC', 'ETH']):
            result = self.adapter.symbol_exists('BTC')
            self.assertTrue(result)
    
    def test_symbol_exists_false(self):
        """Test symbol_exists when symbol doesn't exist"""
        with patch.object(self.adapter, 'get_symbol_list', return_value=['BTC', 'ETH']):
            result = self.adapter.symbol_exists('LTC')
            self.assertFalse(result)
    
    def test_backup_database_not_implemented(self):
        """Test that backup_database returns False (not implemented)"""
        result = self.adapter.backup_database()
        self.assertFalse(result)
    
    def test_get_data_range_not_implemented(self):
        """Test that get_data_range returns None (not implemented)"""
        result = self.adapter.get_data_range('BTC')
        self.assertIsNone(result)
    
    def test_get_latest_data_not_implemented(self):
        """Test that get_latest_data returns None (not implemented)"""
        result = self.adapter.get_latest_data('BTC')
        self.assertIsNone(result)
    
    def test_cleanup_old_data_not_implemented(self):
        """Test that cleanup_old_data returns 0 (not implemented)"""
        result = self.adapter.cleanup_old_data()
        self.assertEqual(result, 0)
    
    def test_optimize_database_not_implemented(self):
        """Test that optimize_database returns False (not implemented)"""
        result = self.adapter.optimize_database()
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
.client.Dispatch')
    def test_init(self, mock_dispatch):
        """Test adapter initialization"""
        adapter = AmiBrokerAdapter(self.mock_config)
        
        self.assertIsNotNone(adapter.config)
        self.assertIsNone(adapter.database_path)
        self.assertFalse(adapter.connection_verified)
    
    @patch('win32com.client.Dispatch')
    def test_connect_success_with_database_path(self, mock_dispatch):
        """Test successful connection with database path"""
        # Create mock COM object
        mock_com = Mock()
        mock_com.LoadDatabase.return_value = True
        mock_dispatch.return_value = mock_com
        
        # Create test database file
        with open(self.test_db_path, 'w') as f:
            f.write("test database")
        
        adapter = AmiBrokerAdapter(self.mock_config)
        
        with patch.object(adapter, '_get_current_database', return_value=None):
            result = adapter.connect(self.test_db_path)
        
        self.assertTrue(result)
        self.assertTrue(adapter.connection_verified)
        self.assertEqual(adapter.database_path, self.test_db_path)
        mock_com.LoadDatabase.assert_called_once_with(self.test_db_path)
    
    @patch('win32com.client.Dispatch')
    def test_connect_database_not_exists(self, mock_dispatch):
        """Test connection failure when database doesn't exist"""
        mock_dispatch.return_value = Mock()
        
        adapter = AmiBrokerAdapter(self.mock_config)
        result = adapter.connect("/nonexistent/path/test.adb")
        
        self.assertFalse(result)
        self.assertFalse(adapter.connection_verified)
    
    @patch('win32com.client.Dispatch')
    def test_connect_com_failure(self, mock_dispatch):
        """Test connection failure when COM dispatch fails"""
        mock_dispatch.side_effect = Exception("COM error")
        
        adapter = AmiBrokerAdapter(self.mock_config)
        result = adapter.connect(self.test_db_path)
        
        self.assertFalse(result)
    
    @patch('win32com.client.Dispatch')
    def test_create_database_success(self, mock_dispatch):
        """Test successful database creation"""
        mock_com = Mock()
        mock_com.NewDatabase.return_value = True
        mock_dispatch.return_value = mock_com
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        result = adapter.create_database(self.test_db_path)
        
        self.assertTrue(result)
        self.assertEqual(adapter.database_path, self.test_db_path)
        self.assertTrue(adapter.connection_verified)
        mock_com.NewDatabase.assert_called_once_with(self.test_db_path)
    
    @patch('win32com.client.Dispatch')
    def test_create_database_already_exists(self, mock_dispatch):
        """Test database creation when database already exists"""
        # Create existing database file
        with open(self.test_db_path, 'w') as f:
            f.write("existing database")
        
        adapter = AmiBrokerAdapter(self.mock_config)
        result = adapter.create_database(self.test_db_path)
        
        self.assertFalse(result)
    
    @patch('win32com.client.Dispatch')
    def test_create_database_creates_directory(self, mock_dispatch):
        """Test that database creation creates necessary directories"""
        mock_com = Mock()
        mock_com.NewDatabase.return_value = True
        mock_dispatch.return_value = mock_com
        
        # Use nested directory path
        nested_db_path = os.path.join(self.temp_dir, "nested", "dir", "test.adb")
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        result = adapter.create_database(nested_db_path)
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(os.path.dirname(nested_db_path)))
    
    def test_validate_data_format_valid(self):
        """Test data format validation with valid data"""
        adapter = AmiBrokerAdapter(self.mock_config)
        
        result = adapter.validate_data_format(self.test_data)
        
        self.assertTrue(result)
    
    def test_validate_data_format_missing_columns(self):
        """Test data format validation with missing columns"""
        adapter = AmiBrokerAdapter(self.mock_config)
        
        # Create data missing required columns
        invalid_data = pd.DataFrame({
            'Open': [100.0, 101.0],
            'High': [105.0, 106.0]
            # Missing Low, Close, Volume
        }, index=pd.date_range('2023-01-01', periods=2, freq='D'))
        
        result = adapter.validate_data_format(invalid_data)
        
        self.assertFalse(result)
    
    def test_validate_data_format_wrong_index_type(self):
        """Test data format validation with wrong index type"""
        adapter = AmiBrokerAdapter(self.mock_config)
        
        # Create data with non-datetime index
        invalid_data = pd.DataFrame({
            'Open': [100.0, 101.0],
            'High': [105.0, 106.0],
            'Low': [95.0, 96.0],
            'Close': [104.0, 105.0],
            'Volume': [1000000, 1100000]
        }, index=[0, 1])  # Integer index instead of datetime
        
        result = adapter.validate_data_format(invalid_data)
        
        self.assertFalse(result)
    
    def test_validate_data_format_non_numeric_data(self):
        """Test data format validation with non-numeric data"""
        adapter = AmiBrokerAdapter(self.mock_config)
        
        # Create data with non-numeric values
        invalid_data = pd.DataFrame({
            'Open': ['invalid', 'data'],
            'High': [105.0, 106.0],
            'Low': [95.0, 96.0],
            'Close': [104.0, 105.0],
            'Volume': [1000000, 1100000]
        }, index=pd.date_range('2023-01-01', periods=2, freq='D'))
        
        result = adapter.validate_data_format(invalid_data)
        
        self.assertFalse(result)
    
    def test_validate_data_format_empty_dataframe(self):
        """Test data format validation with empty DataFrame"""
        adapter = AmiBrokerAdapter(self.mock_config)
        
        empty_data = pd.DataFrame()
        result = adapter.validate_data_format(empty_data)
        
        self.assertFalse(result)
    
    def test_validate_data_format_not_dataframe(self):
        """Test data format validation with non-DataFrame input"""
        adapter = AmiBrokerAdapter(self.mock_config)
        
        result = adapter.validate_data_format("not a dataframe")
        
        self.assertFalse(result)
    
    @patch('win32com.client.Dispatch')
    def test_import_data_success(self, mock_dispatch):
        """Test successful data import"""
        # Setup mock COM objects
        mock_com = Mock()
        mock_stock = Mock()
        mock_quotations = Mock()
        mock_quote = Mock()
        
        mock_com.Stocks.return_value = mock_stock
        mock_stock.Quotations = mock_quotations
        mock_quotations.Add.return_value = mock_quote
        mock_dispatch.return_value = mock_com
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        metadata = {
            'full_name': 'Test Coin',
            'group_id': 253,
            'market_id': 1
        }
        
        result = adapter.import_data('TEST', self.test_data, metadata)
        
        self.assertTrue(result)
        mock_com.Stocks.assert_called_once_with('TEST')
        self.assertEqual(mock_quotations.Add.call_count, len(self.test_data))
        mock_stock.Save.assert_called_once()
    
    @patch('win32com.client.Dispatch')
    def test_import_data_invalid_format(self, mock_dispatch):
        """Test data import with invalid data format"""
        adapter = AmiBrokerAdapter(self.mock_config)
        
        invalid_data = pd.DataFrame({'invalid': ['data']})
        result = adapter.import_data('TEST', invalid_data)
        
        self.assertFalse(result)
    
    @patch('win32com.client.Dispatch')
    def test_get_existing_range_with_data(self, mock_dispatch):
        """Test getting existing data range when data exists"""
        # Setup mock quotations
        mock_com = Mock()
        mock_stock = Mock()
        mock_quotations = Mock()
        mock_quotations.Count = 3
        
        # Mock individual quotes
        mock_first_quote = Mock()
        mock_first_quote.Date.year = 2023
        mock_first_quote.Date.month = 1
        mock_first_quote.Date.day = 1
        
        mock_last_quote = Mock()
        mock_last_quote.Date.year = 2023
        mock_last_quote.Date.month = 1
        mock_last_quote.Date.day = 3
        
        mock_quotations.side_effect = lambda i: mock_first_quote if i == 0 else mock_last_quote
        
        mock_stock.Quotations = mock_quotations
        mock_com.Stocks.return_value = mock_stock
        mock_dispatch.return_value = mock_com
        
        adapter = AmiBrokerAdapter(self.mock_config)
        adapter.com_object = mock_com
        
        start_date, end_date = adapter.get_existing_range('TEST')
        
        self.assertIsNotNone(start_date)
        self.assertIsNotNone(end_date)
        self.assertEqual(start_date.date(), datetime(2023, 1, 1).date())
        self.assertEqual(end_date.date(), datetime(2023, 1, 3).date())
    
    @patch('win32com