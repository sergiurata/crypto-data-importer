"""
Test cases for CoinGeckoProvider
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import requests
import time
import sys
import os
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

# Mock pandas before importing CoinGeckoProvider
with patch.dict('sys.modules', {'pandas': MagicMock()}):
    from providers.coingecko_provider import CoinGeckoProvider
    from core.configuration_manager import ConfigurationManager


class TestCoinGeckoProvider(unittest.TestCase):
    """Test cases for CoinGeckoProvider class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create mock configuration
        self.mock_config = Mock(spec=ConfigurationManager)
        self.mock_config.get.return_value = ""
        self.mock_config.getfloat.return_value = 1.5
        self.mock_config.getint.return_value = 30
        
        # Create provider instance
        self.provider = CoinGeckoProvider(self.mock_config)
    
    def test_init_without_api_key(self):
        """Test initialization without API key"""
        self.mock_config.get.return_value = ""
        
        provider = CoinGeckoProvider(self.mock_config)
        
        # Verify API key header is not set
        self.assertNotIn('x-cg-demo-api-key', provider.session.headers)
        self.assertEqual(provider.base_url, "https://api.coingecko.com/api/v3")
    
    def test_init_with_api_key(self):
        """Test initialization with API key"""
        self.mock_config.get.side_effect = lambda section, key: "test_api_key" if key == "coingecko_api_key" else ""
        
        provider = CoinGeckoProvider(self.mock_config)
        
        # Verify API key header is set
        self.assertEqual(provider.session.headers.get('x-cg-demo-api-key'), "test_api_key")
    
    @patch('time.time')
    def test_handle_rate_limiting(self, mock_time):
        """Test rate limiting functionality"""
        # Mock time progression: first call returns current time, second call returns time after sleep
        mock_time.side_effect = [0.5, 1.5]  # 0.5 seconds passed, need to sleep 1.0 more
        
        self.provider.last_request_time = 0
        self.provider.rate_limit_delay = 1.5
        
        with patch('time.sleep') as mock_sleep:
            self.provider.handle_rate_limiting()
            mock_sleep.assert_called_once_with(1.0)  # Should sleep for remaining time
    
    def test_validate_response_success(self):
        """Test response validation for successful response"""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        
        result = self.provider.validate_response(mock_response)
        self.assertTrue(result)
    
    def test_validate_response_failure(self):
        """Test response validation for failed response"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        
        result = self.provider.validate_response(mock_response)
        self.assertFalse(result)
    
    @patch('time.time')
    def test_get_all_coins_success(self, mock_time):
        """Test successful retrieval of all coins"""
        mock_time.return_value = 1000
        
        # Mock successful response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = [
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"},
            {"id": "ethereum", "symbol": "eth", "name": "Ethereum"}
        ]
        
        with patch.object(self.provider.session, 'get', return_value=mock_response):
            result = self.provider.get_all_coins()
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], "bitcoin")
        self.assertEqual(result[1]["id"], "ethereum")
    
    @patch('time.time')
    def test_get_all_coins_failure(self, mock_time):
        """Test handling of failed coin retrieval"""
        mock_time.return_value = 1000
        
        # Mock failed response
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")
        
        with patch.object(self.provider.session, 'get', return_value=mock_response):
            result = self.provider.get_all_coins()
        
        self.assertEqual(result, [])
    
    @patch('time.time')
    def test_get_market_data_success(self, mock_time):
        """Test successful market data retrieval"""
        mock_time.return_value = 1000
        
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "prices": [[1609459200000, 29000.0], [1609545600000, 30000.0]],
            "market_caps": [[1609459200000, 540000000000], [1609545600000, 560000000000]],
            "total_volumes": [[1609459200000, 50000000000], [1609545600000, 52000000000]]
        }
        
        with patch.object(self.provider.session, 'get', return_value=mock_response):
            result = self.provider.get_market_data("bitcoin", 30)
        
        self.assertIsNotNone(result)
        self.assertIn("prices", result)
        self.assertIn("market_caps", result)
        self.assertIn("total_volumes", result)
    
    def test_get_market_data_failure(self):
        """Test handling of failed market data retrieval"""
        with patch.object(self.provider, 'retry_request', return_value=None):
            result = self.provider.get_market_data("invalid_coin")
        
        self.assertIsNone(result)
    
    @patch('time.time')
    def test_get_exchange_data_success(self, mock_time):
        """Test successful exchange data retrieval"""
        mock_time.return_value = 1000
        
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "tickers": [
                {
                    "base": "BTC",
                    "target": "USD",
                    "market": {"identifier": "kraken"},
                    "trade_url": "https://trade.kraken.com"
                }
            ]
        }
        
        with patch.object(self.provider.session, 'get', return_value=mock_response):
            result = self.provider.get_exchange_data("bitcoin")
        
        self.assertIsNotNone(result)
        self.assertEqual(result["id"], "bitcoin")
        self.assertIn("tickers", result)
    
    @patch('time.time')
    def test_get_coin_details_success(self, mock_time):
        """Test successful coin details retrieval"""
        mock_time.return_value = 1000
        
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "market_data": {
                "current_price": {"usd": 50000},
                "market_cap": {"usd": 950000000000}
            }
        }
        
        with patch.object(self.provider.session, 'get', return_value=mock_response):
            result = self.provider.get_coin_details("bitcoin")
        
        self.assertIsNotNone(result)
        self.assertEqual(result["id"], "bitcoin")
        self.assertIn("market_data", result)
    
    def test_format_market_data_success(self):
        """Test successful market data formatting"""
        # Mock pandas at the instance level since it's already mocked globally
        coin_data = {
            "prices": [[1609459200000, 29000.0], [1609545600000, 30000.0]],
            "market_caps": [[1609459200000, 540000000000], [1609545600000, 560000000000]],
            "total_volumes": [[1609459200000, 50000000000], [1609545600000, 52000000000]]
        }
        
        coin_info = {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"}
        
        # Since pandas is mocked globally, this should work
        result = self.provider.format_market_data(coin_data, coin_info)
        
        # The method should return something since pandas is mocked
        self.assertIsNotNone(result)
    
    def test_format_market_data_empty_prices(self):
        """Test formatting with empty prices data"""
        coin_data = {"prices": [], "market_caps": [], "total_volumes": []}
        coin_info = {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"}
        
        result = self.provider.format_market_data(coin_data, coin_info)
        
        self.assertIsNone(result)
    
    def test_format_market_data_exception(self):
        """Test formatting with invalid data causing exception"""
        coin_data = {"invalid": "data"}
        coin_info = {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"}
        
        result = self.provider.format_market_data(coin_data, coin_info)
        
        self.assertIsNone(result)
    
    def test_retry_request_success_first_attempt(self):
        """Test retry mechanism succeeding on first attempt"""
        mock_func = Mock(return_value="success")
        
        result = self.provider.retry_request(mock_func, "arg1", kwarg1="value1")
        
        self.assertEqual(result, "success")
        mock_func.assert_called_once_with("arg1", kwarg1="value1")
    
    @patch('time.sleep')
    def test_retry_request_success_after_retry(self, mock_sleep):
        """Test retry mechanism succeeding after one retry"""
        mock_func = Mock()
        mock_func.side_effect = [requests.exceptions.RequestException("Error"), "success"]
        
        self.provider.retry_attempts = 3
        
        with patch.object(self.provider, 'handle_rate_limiting'):  # Mock rate limiting to avoid its sleep
            result = self.provider.retry_request(mock_func, "arg1")
        
        self.assertEqual(result, "success")
        self.assertEqual(mock_func.call_count, 2)
        mock_sleep.assert_called_once_with(1)  # 2^0 = 1
    
    @patch('time.sleep')
    def test_retry_request_all_attempts_fail(self, mock_sleep):
        """Test retry mechanism when all attempts fail"""
        mock_func = Mock()
        mock_func.side_effect = requests.exceptions.RequestException("Error")
        
        self.provider.retry_attempts = 2
        
        with patch.object(self.provider, 'handle_rate_limiting'):  # Mock rate limiting to avoid its sleep
            result = self.provider.retry_request(mock_func, "arg1")
        
        self.assertIsNone(result)
        self.assertEqual(mock_func.call_count, 2)
        mock_sleep.assert_called_once_with(1)  # Only one retry sleep call
    
    def test_retry_request_unexpected_exception(self):
        """Test retry mechanism with unexpected exception"""
        mock_func = Mock()
        mock_func.side_effect = ValueError("Unexpected error")
        
        result = self.provider.retry_request(mock_func, "arg1")
        
        self.assertIsNone(result)
        mock_func.assert_called_once()
    
    def test_get_default_headers(self):
        """Test default headers generation"""
        headers = self.provider.get_default_headers()
        
        self.assertIn('User-Agent', headers)
        self.assertIn('Accept', headers)
        self.assertIn('Content-Type', headers)
        self.assertEqual(headers['Accept'], 'application/json')
    
    def test_log_api_usage(self):
        """Test API usage logging"""
        # Test that the method doesn't crash and can be called with different parameters
        try:
            self.provider.log_api_usage("test_endpoint", "success", 1.5)
            self.provider.log_api_usage("another_endpoint", "failure")
            self.provider.log_api_usage("third_endpoint", "success", None)
        except Exception as e:
            self.fail(f"log_api_usage raised an exception: {e}")
    
    @patch('time.time')
    def test_get_api_status_success(self, mock_time):
        """Test API status retrieval"""
        mock_time.return_value = 1000
        
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"gecko_says": "(V3) To the Moon!"}
        mock_response.headers = {
            'x-ratelimit-remaining': '50',
            'x-ratelimit-reset': '1609459200'
        }
        
        with patch.object(self.provider.session, 'get', return_value=mock_response):
            result = self.provider.get_api_status()
        
        self.assertIn("gecko_says", result)
        self.assertIn("rate_limit_remaining", result)
        self.assertEqual(result["rate_limit_remaining"], "50")
    
    def test_get_api_status_failure(self):
        """Test API status retrieval failure"""
        with patch.object(self.provider, 'retry_request', return_value=None):
            result = self.provider.get_api_status()
            self.assertEqual(result, {'status': 'error'})
    
    def test_rate_limit_handling_no_delay(self):
        """Test rate limiting when no delay is needed"""
        self.provider.last_request_time = 0
        self.provider.rate_limit_delay = 1.0
        
        with patch('time.time', return_value=2.0):  # More than delay
            with patch('time.sleep') as mock_sleep:
                self.provider.handle_rate_limiting()
                mock_sleep.assert_not_called()
    
    def test_rate_limit_handling_with_delay(self):
        """Test rate limiting when delay is needed"""
        self.provider.last_request_time = 1.0
        self.provider.rate_limit_delay = 2.0
        
        with patch('time.time', return_value=1.5):  # 0.5 seconds passed, need 1.5 more
            with patch('time.sleep') as mock_sleep:
                self.provider.handle_rate_limiting()
                mock_sleep.assert_called_once_with(1.5)
    
    def test_validate_coin_id_success(self):
        """Test coin ID validation for valid coin"""
        with patch.object(self.provider, 'retry_request', return_value=True):
            result = self.provider.validate_coin_id("bitcoin")
            self.assertTrue(result)
    
    def test_validate_coin_id_failure(self):
        """Test coin ID validation for invalid coin"""
        with patch.object(self.provider, 'retry_request', return_value=None):
            result = self.provider.validate_coin_id("invalid_coin")
            self.assertFalse(result)
    
    @patch('time.time')
    def test_get_exchanges_list_success(self, mock_time):
        """Test successful exchanges list retrieval"""
        mock_time.return_value = 1000
        
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = [
            {"id": "kraken", "name": "Kraken", "year_established": 2011},
            {"id": "binance", "name": "Binance", "year_established": 2017}
        ]
        
        with patch.object(self.provider.session, 'get', return_value=mock_response):
            result = self.provider.get_exchanges_list()
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], "kraken")
        self.assertEqual(result[1]["id"], "binance")


class TestCoinGeckoProviderIntegration(unittest.TestCase):
    """Integration tests for CoinGeckoProvider (requires network access)"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create real configuration for integration tests
        self.mock_config = Mock(spec=ConfigurationManager)
        self.mock_config.get.return_value = ""
        self.mock_config.getfloat.return_value = 2.0  # Slower for integration tests
        self.mock_config.getint.return_value = 60
        
        self.provider = CoinGeckoProvider(self.mock_config)
    
    @unittest.skipIf(os.getenv('SKIP_INTEGRATION_TESTS'), "Skipping integration tests")
    def test_real_api_status(self):
        """Test real API status check (requires internet connection)"""
        result = self.provider.get_api_status()
        
        # Should get a successful response
        self.assertIsInstance(result, dict)
        # CoinGecko ping endpoint returns specific message
        self.assertIn("gecko_says", result)
    
    @unittest.skipIf(os.getenv('SKIP_INTEGRATION_TESTS'), "Skipping integration tests")
    def test_real_coins_list(self):
        """Test retrieving real coins list (requires internet connection)"""
        result = self.provider.get_all_coins()
        
        # Should get a list of coins
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 1000)  # CoinGecko has thousands of coins
        
        # Check structure of first coin
        if result:
            coin = result[0]
            self.assertIn('id', coin)
            self.assertIn('symbol', coin)
            self.assertIn('name', coin)
    
    @unittest.skipIf(os.getenv('SKIP_INTEGRATION_TESTS'), "Skipping integration tests")
    def test_real_bitcoin_market_data(self):
        """Test retrieving real Bitcoin market data (requires internet connection)"""
        result = self.provider.get_market_data("bitcoin", 7)  # Last 7 days
        
        # Should get market data
        self.assertIsInstance(result, dict)
        self.assertIn('prices', result)
        self.assertIn('market_caps', result)
        self.assertIn('total_volumes', result)
        
        # Should have data for 7+ days
        self.assertGreater(len(result['prices']), 5)


if __name__ == '__main__':
    unittest.main()