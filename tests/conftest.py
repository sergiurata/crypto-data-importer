"""
PyTest Configuration and Fixtures
Shared test configuration for the entire test suite
"""

import pytest
import tempfile
import os
import sys
import pandas as pd
from unittest.mock import Mock
from datetime import datetime, timedelta
from pathlib import Path

# Add src to Python path
src_path = Path(__file__).parent.parent / "src"
if src_path.exists():
    sys.path.insert(0, str(src_path))


@pytest.fixture(scope="session")
def project_root():
    """Project root directory"""
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def src_path(project_root):
    """Source code directory"""
    return project_root / "src"


@pytest.fixture
def temp_dir():
    """Temporary directory for test files"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def sample_config_data():
    """Sample configuration data for testing"""
    return {
        'DATABASE': {
            'database_path': r'C:\Test\test.adb',
            'create_if_not_exists': 'true'
        },
        'IMPORT': {
            'max_coins': '100',
            'min_market_cap': '1000000',
            'rate_limit_delay': '1.0'
        },
        'FILTERING': {
            'exclude_stablecoins': 'true',
            'excluded_symbols': 'USDT,USDC'
        }
    }


@pytest.fixture
def mock_config():
    """Mock configuration manager"""
    from core.configuration_manager import ConfigurationManager
    
    config = Mock(spec=ConfigurationManager)
    config.get.return_value = ""
    config.getint.return_value = 0
    config.getfloat.return_value = 0.0
    config.getboolean.return_value = False
    config.getlist.return_value = []
    
    return config


@pytest.fixture
def sample_market_data():
    """Sample market data from CoinGecko API"""
    return {
        "prices": [
            [1609459200000, 29000.0],  # 2021-01-01
            [1609545600000, 30000.0],  # 2021-01-02
            [1609632000000, 31000.0]   # 2021-01-03
        ],
        "market_caps": [
            [1609459200000, 540000000000],
            [1609545600000, 560000000000],
            [1609632000000, 580000000000]
        ],
        "total_volumes": [
            [1609459200000, 50000000000],
            [1609545600000, 52000000000],
            [1609632000000, 54000000000]
        ]
    }


@pytest.fixture
def sample_dataframe():
    """Sample pandas DataFrame with OHLCV data"""
    return pd.DataFrame({
        'Open': [29000.0, 30000.0, 31000.0],
        'High': [29500.0, 30500.0, 31500.0],
        'Low': [28500.0, 29500.0, 30500.0],
        'Close': [29000.0, 30000.0, 31000.0],
        'Volume': [50000000000, 52000000000, 54000000000],
        'MarketCap': [540000000000, 560000000000, 580000000000]
    }, index=pd.date_range('2021-01-01', periods=3, freq='D'))


@pytest.fixture
def sample_kraken_assets():
    """Sample Kraken assets data"""
    return {
        "XXBT": {
            "aclass": "currency",
            "altname": "XBT",
            "decimals": 10,
            "display_decimals": 5
        },
        "XETH": {
            "aclass": "currency", 
            "altname": "ETH",
            "decimals": 10,
            "display_decimals": 5
        },
        "ZUSD": {
            "aclass": "currency",
            "altname": "USD", 
            "decimals": 4,
            "display_decimals": 2
        }
    }


@pytest.fixture
def sample_kraken_pairs():
    """Sample Kraken trading pairs data"""
    return {
        "XXBTZUSD": {
            "altname": "XBTUSD",
            "wsname": "XBT/USD",
            "aclass_base": "currency",
            "base": "XXBT",
            "aclass_quote": "currency", 
            "quote": "ZUSD",
            "lot": "unit",
            "pair_decimals": 1,
            "lot_decimals": 8,
            "lot_multiplier": 1,
            "leverage_buy": [2, 3, 4, 5],
            "leverage_sell": [2, 3, 4, 5],
            "fees": [[0, 0.26], [50000, 0.24], [100000, 0.22]],
            "fees_maker": [[0, 0.16], [50000, 0.14], [100000, 0.12]],
            "fee_volume_currency": "ZUSD",
            "margin_call": 80,
            "margin_stop": 40,
            "ordermin": "0.0001"
        },
        "XETHZUSD": {
            "altname": "ETHUSD",
            "wsname": "ETH/USD", 
            "aclass_base": "currency",
            "base": "XETH",
            "aclass_quote": "currency",
            "quote": "ZUSD",
            "lot": "unit",
            "pair_decimals": 2,
            "lot_decimals": 8,
            "lot_multiplier": 1,
            "leverage_buy": [2, 3, 4, 5],
            "leverage_sell": [2, 3, 4, 5],
            "fees": [[0, 0.26], [50000, 0.24], [100000, 0.22]],
            "fees_maker": [[0, 0.16], [50000, 0.14], [100000, 0.12]],
            "fee_volume_currency": "ZUSD",
            "margin_call": 80,
            "margin_stop": 40,
            "ordermin": "0.001"
        }
    }


@pytest.fixture
def mock_data_provider():
    """Mock data provider for testing"""
    from providers.abstract_data_provider import AbstractDataProvider
    
    provider = Mock(spec=AbstractDataProvider)
    provider.get_all_coins.return_value = []
    provider.get_market_data.return_value = None
    provider.get_exchange_data.return_value = None
    provider.format_market_data.return_value = None
    
    return provider


@pytest.fixture
def mock_exchange_mapper():
    """Mock exchange mapper for testing"""
    from mappers.abstract_exchange_mapper import AbstractExchangeMapper
    
    mapper = Mock(spec=AbstractExchangeMapper)
    mapper.get_exchange_name.return_value = "test_exchange"
    mapper.load_exchange_data.return_value = True
    mapper.map_coin_to_exchange.return_value = None
    mapper.is_tradeable.return_value = False
    mapper.get_symbol_mapping.return_value = None
    mapper.build_mapping.return_value = {}
    mapper.validate_mapping.return_value = True
    
    return mapper


@pytest.fixture
def mock_database_adapter():
    """Mock database adapter for testing"""
    from adapters.abstract_database_adapter import AbstractDatabaseAdapter
    
    adapter = Mock(spec=AbstractDatabaseAdapter)
    adapter.connect.return_value = True
    adapter.create_database.return_value = True
    adapter.import_data.return_value = True
    adapter.update_data.return_value = (10, 2)
    adapter.get_existing_range.return_value = (None, None)
    adapter.create_groups.return_value = True
    adapter.validate_connection.return_value = True
    adapter.get_symbol_list.return_value = []
    adapter.symbol_exists.return_value = False
    adapter.get_symbol_metadata.return_value = {}
    
    return adapter


@pytest.fixture
def mock_amibroker_com():
    """Mock AmiBroker COM object for testing"""
    com_mock = Mock()
    
    # Mock Stocks collection
    stock_mock = Mock()
    stock_mock.FullName = "Test Stock"
    stock_mock.GroupID = 1
    stock_mock.MarketID = 1
    stock_mock.Ticker = "TEST"
    
    # Mock Quotations collection
    quotations_mock = Mock()
    quotations_mock.Count = 0
    quote_mock = Mock()
    quote_mock.Date.year = 2023
    quote_mock.Date.month = 1
    quote_mock.Date.day = 1
    quote_mock.Open = 100.0
    quote_mock.High = 105.0
    quote_mock.Low = 95.0
    quote_mock.Close = 104.0
    quote_mock.Volume = 1000000
    
    quotations_mock.Add.return_value = quote_mock
    quotations_mock.side_effect = lambda i: quote_mock
    stock_mock.Quotations = quotations_mock
    
    # Mock Groups collection
    group_mock = Mock()
    groups_mock = Mock()
    groups_mock.side_effect = lambda i: group_mock
    
    com_mock.Stocks.return_value = stock_mock
    com_mock.Stocks.Count = 1
    com_mock.Stocks.side_effect = lambda i: stock_mock
    com_mock.Groups = groups_mock
    com_mock.LoadDatabase.return_value = True
    com_mock.NewDatabase.return_value = True
    com_mock.DatabasePath = r"C:\Test\test.adb"
    
    return com_mock


@pytest.fixture
def sample_exchange_info():
    """Sample exchange information"""
    from mappers.abstract_exchange_mapper import ExchangeInfo
    
    return ExchangeInfo(
        exchange_name="kraken",
        symbol="BTCUSD",
        pair_name="XXBTZUSD",
        base_currency="BTC",
        target_currency="USD",
        alt_name="XBTUSD",
        trade_url="https://trade.kraken.com/markets/kraken/btc/usd",
        is_active=True,
        min_order_size=0.0001,
        fee_percent=0.26
    )


@pytest.fixture
def sample_import_result():
    """Sample import result for testing"""
    from orchestrators.import_orchestrator import ImportResult
    
    return ImportResult(
        total_processed=100,
        new_records=500,
        updated_records=25,
        failed_count=5,
        kraken_count=30,
        skipped_count=10,
        execution_time=120.5,
        errors=["Sample error 1", "Sample error 2"]
    )


@pytest.fixture(scope="session")
def enable_integration_tests():
    """Enable integration tests if environment variable is set"""
    return not os.getenv('SKIP_INTEGRATION_TESTS', False)


@pytest.fixture
def mock_requests_session():
    """Mock requests session for API testing"""
    session_mock = Mock()
    response_mock = Mock()
    response_mock.status_code = 200
    response_mock.raise_for_status.return_value = None
    response_mock.json.return_value = {"test": "data"}
    response_mock.headers = {}
    
    session_mock.get.return_value = response_mock
    session_mock.post.return_value = response_mock
    session_mock.headers = {}
    
    return session_mock


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (requires network)"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers automatically"""
    for item in items:
        # Add integration marker for tests with 'integration' in name
        if 'integration' in item.nodeid.lower():
            item.add_marker(pytest.mark.integration)
        
        # Add unit marker for all other tests
        else:
            item.add_marker(pytest.mark.unit)
        
        # Add slow marker for tests that might be slow
        if any(keyword in item.nodeid.lower() for keyword in ['test_full_workflow', 'test_real_api', 'test_complete']):
            item.add_marker(pytest.mark.slow)


@pytest.fixture(autouse=True)
def setup_test_environment():
    """Automatically setup test environment for each test"""
    # Set test environment variables
    os.environ['TESTING'] = '1'
    os.environ['LOG_LEVEL'] = 'DEBUG'
    
    yield
    
    # Cleanup after test
    if 'TESTING' in os.environ:
        del os.environ['TESTING']
    if 'LOG_LEVEL' in os.environ:
        del os.environ['LOG_LEVEL']


# Custom pytest markers for better test organization
pytestmark = [
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),
    pytest.mark.filterwarnings("ignore::PendingDeprecationWarning"),
]

