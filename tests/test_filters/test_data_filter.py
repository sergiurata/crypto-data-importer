"""
Test cases for DataFilter
"""

import unittest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from src.filters.data_filter import DataFilter, FilterRule
from src.core.configuration_manager import ConfigurationManager


class TestFilterRule(unittest.TestCase):
    """Test cases for FilterRule dataclass"""
    
    def test_filter_rule_creation(self):
        """Test creating a FilterRule"""
        filter_func = lambda x: x['price'] > 100
        rule = FilterRule(
            name="price_filter",
            filter_func=filter_func,
            description="Price must be greater than 100",
            enabled=True
        )
        
        self.assertEqual(rule.name, "price_filter")
        self.assertEqual(rule.description, "Price must be greater than 100")
        self.assertTrue(rule.enabled)
        self.assertEqual(rule.filter_func, filter_func)
    
    def test_filter_rule_default_enabled(self):
        """Test FilterRule default enabled state"""
        rule = FilterRule(
            name="test",
            filter_func=lambda x: True,
            description="Test filter"
        )
        
        self.assertTrue(rule.enabled)


class TestDataFilter(unittest.TestCase):
    """Test cases for DataFilter class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create mock configuration
        self.mock_config = Mock(spec=ConfigurationManager)
        self.mock_config.getfloat.return_value = 0.0
        self.mock_config.getlist.return_value = []
        self.mock_config.getboolean.return_value = False
        
        # Test coin data
        self.test_coin_data = {
            'symbol': 'BTC',
            'name': 'Bitcoin',
            'market_cap': 500000000000,  # 500B
            'volume_24h': 30000000000,   # 30B
            'price': 50000.0
        }
        
        self.filter = DataFilter(self.mock_config)
    
    def test_init_creates_default_filters(self):
        """Test that initialization creates default filters from config"""
        # Setup config to return some values
        self.mock_config.getfloat.side_effect = lambda section, key, default=0: {
            ('FILTERING', 'min_market_cap'): 1000000000,  # 1B
            ('FILTERING', 'min_volume_24h'): 100000000    # 100M
        }.get((section, key), default)
        
        filter_instance = DataFilter(self.mock_config)
        
        # Should have created market cap and volume filters
        filter_names = [f.name for f in filter_instance.filters]
        self.assertIn('market_cap', filter_names)
        self.assertIn('volume_24h', filter_names)
    
    def test_apply_filters_pass_all(self):
        """Test applying filters when coin passes all filters"""
        # Add a simple filter that should pass
        self.filter.add_filter(FilterRule(
            name="test_filter",
            filter_func=lambda coin: coin['symbol'] == 'BTC',
            description="Must be BTC"
        ))
        
        result = self.filter.apply_filters(self.test_coin_data)
        
        self.assertTrue(result)
    
    def test_apply_filters_fail_one(self):
        """Test applying filters when coin fails one filter"""
        # Add a filter that should fail
        self.filter.add_filter(FilterRule(
            name="fail_filter",
            filter_func=lambda coin: coin['symbol'] == 'ETH',
            description="Must be ETH"
        ))
        
        result = self.filter.apply_filters(self.test_coin_data)
        
        self.assertFalse(result)
    
    def test_apply_filters_disabled_filter(self):
        """Test that disabled filters are ignored"""
        # Add a filter that would fail but is disabled
        failing_filter = FilterRule(
            name="disabled_filter",
            filter_func=lambda coin: False,  # Always fails
            description="Always fails",
            enabled=False
        )
        self.filter.add_filter(failing_filter)
        
        result = self.filter.apply_filters(self.test_coin_data)
        
        self.assertTrue(result)  # Should pass because filter is disabled
    
    def test_apply_filters_exception_handling(self):
        """Test that filter exceptions are handled gracefully"""
        # Add a filter that raises an exception
        self.filter.add_filter(FilterRule(
            name="exception_filter",
            filter_func=lambda coin: coin['nonexistent_key'],  # KeyError
            description="Raises exception"
        ))
        
        result = self.filter.apply_filters(self.test_coin_data)
        
        self.assertFalse(result)  # Should fail gracefully
    
    def test_add_filter(self):
        """Test adding a new filter"""
        initial_count = len(self.filter.filters)
        
        new_filter = FilterRule(
            name="new_filter",
            filter_func=lambda coin: True,
            description="New filter"
        )
        
        self.filter.add_filter(new_filter)
        
        self.assertEqual(len(self.filter.filters), initial_count + 1)
        self.assertIn(new_filter, self.filter.filters)
    
    def test_add_filter_replaces_existing(self):
        """Test that adding filter with existing name replaces it"""
        # Add initial filter
        initial_filter = FilterRule(
            name="test_filter",
            filter_func=lambda coin: True,
            description="Initial filter"
        )
        self.filter.add_filter(initial_filter)
        initial_count = len(self.filter.filters)
        
        # Add filter with same name
        replacement_filter = FilterRule(
            name="test_filter",
            filter_func=lambda coin: False,
            description="Replacement filter"
        )
        self.filter.add_filter(replacement_filter)
        
        # Should have same count (replaced, not added)
        self.assertEqual(len(self.filter.filters), initial_count)
        
        # Should contain replacement filter
        filter_descriptions = [f.description for f in self.filter.filters]
        self.assertIn("Replacement filter", filter_descriptions)
        self.assertNotIn("Initial filter", filter_descriptions)
    
    def test_remove_filter_exists(self):
        """Test removing an existing filter"""
        # Add a filter
        test_filter = FilterRule(
            name="removable_filter",
            filter_func=lambda coin: True,
            description="Removable filter"
        )
        self.filter.add_filter(test_filter)
        initial_count = len(self.filter.filters)
        
        # Remove the filter
        self.filter.remove_filter("removable_filter")
        
        self.assertEqual(len(self.filter.filters), initial_count - 1)
        filter_names = [f.name for f in self.filter.filters]
        self.assertNotIn("removable_filter", filter_names)
    
    def test_remove_filter_not_exists(self):
        """Test removing a non-existent filter"""
        initial_count = len(self.filter.filters)
        
        self.filter.remove_filter("nonexistent_filter")
        
        # Count should remain the same
        self.assertEqual(len(self.filter.filters), initial_count)
    
    def test_enable_filter(self):
        """Test enabling a filter"""
        # Add disabled filter
        test_filter = FilterRule(
            name="test_filter",
            filter_func=lambda coin: True,
            description="Test filter",
            enabled=False
        )
        self.filter.add_filter(test_filter)
        
        # Enable the filter
        self.filter.enable_filter("test_filter")
        
        # Find the filter and check it's enabled
        for f in self.filter.filters:
            if f.name == "test_filter":
                self.assertTrue(f.enabled)
                break
        else:
            self.fail("Filter not found")
    
    def test_disable_filter(self):
        """Test disabling a filter"""
        # Add enabled filter
        test_filter = FilterRule(
            name="test_filter",
            filter_func=lambda coin: True,
            description="Test filter",
            enabled=True
        )
        self.filter.add_filter(test_filter)
        
        # Disable the filter
        self.filter.disable_filter("test_filter")
        
        # Find the filter and check it's disabled
        for f in self.filter.filters:
            if f.name == "test_filter":
                self.assertFalse(f.enabled)
                break
        else:
            self.fail("Filter not found")
    
    def test_validate_market_cap(self):
        """Test market cap validation"""
        # Test valid market cap
        self.assertTrue(self.filter.validate_market_cap(1000000000, 500000000))
        
        # Test invalid market cap
        self.assertFalse(self.filter.validate_market_cap(100000000, 500000000))
        
        # Test edge case
        self.assertTrue(self.filter.validate_market_cap(500000000, 500000000))
    
    def test_validate_volume(self):
        """Test volume validation"""
        # Test valid volume
        self.assertTrue(self.filter.validate_volume(2000000000, 1000000000))
        
        # Test invalid volume
        self.assertFalse(self.filter.validate_volume(500000000, 1000000000))
        
        # Test edge case
        self.assertTrue(self.filter.validate_volume(1000000000, 1000000000))
    
    def test_check_excluded_symbols(self):
        """Test excluded symbols check"""
        excluded_list = ['USDT', 'USDC', 'DAI']
        
        # Test excluded symbol
        self.assertFalse(self.filter.check_excluded_symbols('USDT', excluded_list))
        self.assertFalse(self.filter.check_excluded_symbols('usdt', excluded_list))  # Case insensitive
        
        # Test non-excluded symbol
        self.assertTrue(self.filter.check_excluded_symbols('BTC', excluded_list))
        
        # Test empty exclusion list
        self.assertTrue(self.filter.check_excluded_symbols('USDT', []))
    
    def test_check_included_symbols(self):
        """Test included symbols check"""
        included_list = ['BTC', 'ETH', 'ADA']
        
        # Test included symbol
        self.assertTrue(self.filter.check_included_symbols('BTC', included_list))
        self.assertTrue(self.filter.check_included_symbols('btc', included_list))  # Case insensitive
        
        # Test non-included symbol
        self.assertFalse(self.filter.check_included_symbols('DOGE', included_list))
        
        # Test empty inclusion list (should allow all)
        self.assertTrue(self.filter.check_included_symbols('DOGE', []))
    
    def test_exclude_stablecoins(self):
        """Test stablecoin exclusion"""
        # Test obvious stablecoins
        self.assertFalse(self.filter.exclude_stablecoins('USDT', 'Tether USD'))
        self.assertFalse(self.filter.exclude_stablecoins('USDC', 'USD Coin'))
        self.assertFalse(self.filter.exclude_stablecoins('DAI', 'Dai Stablecoin'))
        
        # Test non-stablecoins
        self.assertTrue(self.filter.exclude_stablecoins('BTC', 'Bitcoin'))
        self.assertTrue(self.filter.exclude_stablecoins('ETH', 'Ethereum'))
        
        # Test edge cases
        self.assertFalse(self.filter.exclude_stablecoins('EURC', 'Euro Coin'))  # Contains 'eur'
        self.assertTrue(self.filter.exclude_stablecoins('MATIC', 'Polygon'))  # Doesn't contain stablecoin indicators
    
    def test_validate_price_range(self):
        """Test price range validation"""
        # Test valid price
        self.assertTrue(self.filter.validate_price_range(50.0, 10.0, 100.0))
        
        # Test price too low
        self.assertFalse(self.filter.validate_price_range(5.0, 10.0, 100.0))
        
        # Test price too high
        self.assertFalse(self.filter.validate_price_range(150.0, 10.0, 100.0))
        
        # Test edge cases
        self.assertTrue(self.filter.validate_price_range(10.0, 10.0, 100.0))  # At minimum
        self.assertTrue(self.filter.validate_price_range(100.0, 10.0, 100.0))  # At maximum
    
    def test_validate_age(self):
        """Test coin age validation"""
        # Test old enough coin
        old_date = (datetime.now() - timedelta(days=100)).isoformat()
        self.assertTrue(self.filter.validate_age(old_date, 30))
        
        # Test too young coin
        young_date = (datetime.now() - timedelta(days=10)).isoformat()
        self.assertFalse(self.filter.validate_age(young_date, 30))
        
        # Test edge case
        exact_date = (datetime.now() - timedelta(days=30)).isoformat()
        self.assertTrue(self.filter.validate_age(exact_date, 30))
        
        # Test invalid date
        self.assertTrue(self.filter.validate_age("invalid_date", 30))  # Should default to True
        
        # Test empty date
        self.assertTrue(self.filter.validate_age("", 30))  # Should default to True
    
    def test_get_active_filters(self):
        """Test getting active filters"""
        # Add mix of enabled and disabled filters
        enabled_filter = FilterRule("enabled", lambda x: True, "Enabled", enabled=True)
        disabled_filter = FilterRule("disabled", lambda x: True, "Disabled", enabled=False)
        
        self.filter.add_filter(enabled_filter)
        self.filter.add_filter(disabled_filter)
        
        active_filters = self.filter.get_active_filters()
        
        self.assertEqual(len(active_filters), 1)
        self.assertEqual(active_filters[0].name, "enabled")
    
    def test_get_filter_summary(self):
        """Test getting filter summary"""
        # Add mix of enabled and disabled filters
        enabled_filter = FilterRule("enabled", lambda x: True, "Enabled filter", enabled=True)
        disabled_filter = FilterRule("disabled", lambda x: True, "Disabled filter", enabled=False)
        
        self.filter.add_filter(enabled_filter)
        self.filter.add_filter(disabled_filter)
        
        summary = self.filter.get_filter_summary()
        
        self.assertEqual(summary['total_filters'], 2)
        self.assertEqual(summary['active_filters'], 1)
        self.assertIn("Enabled filter", summary['filter_descriptions'])
        self.assertIn("disabled", summary['disabled_filters'])
    
    def test_test_filter(self):
        """Test testing a specific filter"""
        # Add a test filter
        test_filter = FilterRule(
            name="price_filter",
            filter_func=lambda coin: coin['price'] > 1000,
            description="Price > 1000"
        )
        self.filter.add_filter(test_filter)
        
        # Test with data that should pass
        passing_data = {'price': 2000}
        self.assertTrue(self.filter.test_filter("price_filter", passing_data))
        
        # Test with data that should fail
        failing_data = {'price': 500}
        self.assertFalse(self.filter.test_filter("price_filter", failing_data))
        
        # Test with non-existent filter
        self.assertFalse(self.filter.test_filter("nonexistent", passing_data))
    
    def test_create_custom_filter_success(self):
        """Test creating a custom filter successfully"""
        condition = "coin_data['price'] > 1000"
        
        result = self.filter.create_custom_filter("custom_price", condition, "Custom price filter")
        
        self.assertTrue(result)
        
        # Test the created filter
        filter_names = [f.name for f in self.filter.filters]
        self.assertIn("custom_price", filter_names)
        
        # Test the filter works
        self.assertTrue(self.filter.test_filter("custom_price", {'price': 2000}))
        self.assertFalse(self.filter.test_filter("custom_price", {'price': 500}))
    
    def test_create_custom_filter_invalid_syntax(self):
        """Test creating custom filter with invalid syntax"""
        invalid_condition = "coin_data['price'] >" # Invalid syntax
        
        result = self.filter.create_custom_filter("invalid_filter", invalid_condition)
        
        self.assertFalse(result)
    
    def test_create_custom_filter_unsafe_code(self):
        """Test that custom filter prevents unsafe code execution"""
        unsafe_condition = "import os; os.system('rm -rf /')"
        
        result = self.filter.create_custom_filter("unsafe_filter", unsafe_condition)
        
        # Should fail during compilation/testing
        self.assertFalse(result)
    
    def test_load_filters_from_config(self):
        """Test loading custom filters from configuration"""
        # Mock configuration with custom filters section
        mock_config = Mock()
        mock_config.config.has_section.return_value = True
        mock_config.config.options.return_value = ['price_filter', 'volume_filter']
        mock_config.get.side_effect = lambda section, key: {
            ('CUSTOM_FILTERS', 'price_filter'): "coin_data['price'] > 100",
            ('CUSTOM_FILTERS', 'volume_filter'): "coin_data['volume'] > 1000000"
        }.get((section, key), "")
        
        filter_instance = DataFilter(mock_config)
        filter_instance.load_filters_from_config()
        
        filter_names = [f.name for f in filter_instance.filters]
        self.assertIn('price_filter', filter_names)
        self.assertIn('volume_filter', filter_names)
    
    def test_export_filter_config(self):
        """Test exporting filter configuration"""
        # Add some filters
        self.filter.add_filter(FilterRule("test1", lambda x: True, "Test 1", enabled=True))
        self.filter.add_filter(FilterRule("test2", lambda x: False, "Test 2", enabled=False))
        
        # Mock config values
        self.mock_config.getfloat.side_effect = lambda section, key, default=0: {
            ('FILTERING', 'min_market_cap'): 1000000,
            ('FILTERING', 'min_volume_24h'): 500000
        }.get((section, key), default)
        
        self.mock_config.getlist.side_effect = lambda section, key: {
            ('FILTERING', 'excluded_symbols'): ['USDT', 'USDC'],
            ('FILTERING', 'included_symbols'): []
        }.get((section, key), [])
        
        self.mock_config.getboolean.return_value = True
        
        exported = self.filter.export_filter_config()
        
        self.assertIn('filters', exported)
        self.assertIn('config_values', exported)
        self.assertEqual(len(exported['filters']), 2)
        self.assertEqual(exported['config_values']['min_market_cap'], 1000000)
    
    def test_clear_all_filters(self):
        """Test clearing all filters"""
        # Add some filters
        self.filter.add_filter(FilterRule("test1", lambda x: True, "Test 1"))
        self.filter.add_filter(FilterRule("test2", lambda x: True, "Test 2"))
        
        initial_count = len(self.filter.filters)
        self.assertGreater(initial_count, 0)
        
        self.filter.clear_all_filters()
        
        self.assertEqual(len(self.filter.filters), 0)
    
    def test_reset_to_defaults(self):
        """Test resetting filters to defaults"""
        # Add custom filters
        self.filter.add_filter(FilterRule("custom", lambda x: True, "Custom"))
        
        # Mock config to return some default values
        self.mock_config.getfloat.return_value = 1000000  # Will create market cap filter
        
        self.filter.reset_to_defaults()
        
        # Should have cleared custom filters and recreated defaults
        filter_names = [f.name for f in self.filter.filters]
        self.assertNotIn("custom", filter_names)
        self.assertIn("market_cap", filter_names)  # Should have default filter


class TestDataFilterIntegration(unittest.TestCase):
    """Integration tests for DataFilter with realistic scenarios"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create realistic configuration
        self.mock_config = Mock(spec=ConfigurationManager)
        self.mock_config.getfloat.side_effect = lambda section, key, default=0: {
            ('FILTERING', 'min_market_cap'): 10000000,  # 10M
            ('FILTERING', 'min_volume_24h'): 1000000    # 1M
        }.get((section, key), default)
        
        self.mock_config.getlist.side_effect = lambda section, key: {
            ('FILTERING', 'excluded_symbols'): ['USDT', 'USDC', 'DAI'],
            ('FILTERING', 'included_symbols'): []
        }.get((section, key), [])
        
        self.mock_config.getboolean.side_effect = lambda section, key, default=False: {
            ('FILTERING', 'exclude_stablecoins'): True
        }.get((section, key), default)
        
        self.filter = DataFilter(self.mock_config)
    
    def test_realistic_coin_filtering_bitcoin(self):
        """Test filtering realistic Bitcoin data"""
        bitcoin_data = {
            'symbol': 'BTC',
            'name': 'Bitcoin',
            'market_cap': 500000000000,  # 500B
            'volume_24h': 30000000000,   # 30B
            'price': 50000.0
        }
        
        result = self.filter.apply_filters(bitcoin_data)
        self.assertTrue(result)  # Should pass all filters
    
    def test_realistic_coin_filtering_stablecoin(self):
        """Test filtering realistic stablecoin data"""
        usdt_data = {
            'symbol': 'USDT',
            'name': 'Tether USD',
            'market_cap': 80000000000,   # 80B
            'volume_24h': 50000000000,   # 50B
            'price': 1.0
        }
        
        result = self.filter.apply_filters(usdt_data)
        self.assertFalse(result)  # Should fail due to stablecoin exclusion and excluded symbols
    
    def test_realistic_coin_filtering_small_cap(self):
        """Test filtering small market cap coin"""
        small_coin_data = {
            'symbol': 'SMALL',
            'name': 'Small Coin',
            'market_cap': 1000000,      # 1M (below threshold)
            'volume_24h': 100000,       # 100K (below threshold)
            'price': 0.01
        }
        
        result = self.filter.apply_filters(small_coin_data)
        self.assertFalse(result)  # Should fail due to low market cap and volume
    
    def test_realistic_coin_filtering_borderline(self):
        """Test filtering coin at threshold boundaries"""
        borderline_data = {
            'symbol': 'BORDER',
            'name': 'Borderline Coin',
            'market_cap': 10000000,     # Exactly at threshold
            'volume_24h': 1000000,      # Exactly at threshold
            'price': 1.0
        }
        
        result = self.filter.apply_filters(borderline_data)
        self.assertTrue(result)  # Should pass (>= threshold)
    
    def test_realistic_filtering_with_custom_rules(self):
        """Test filtering with additional custom rules"""
        # Add custom filter for minimum price
        self.filter.create_custom_filter(
            "min_price",
            "coin_data['price'] >= 0.001",
            "Minimum price filter"
        )
        
        # Test very low price coin
        low_price_data = {
            'symbol': 'LOWPRICE',
            'name': 'Low Price Coin',
            'market_cap': 50000000,     # Above market cap threshold
            'volume_24h': 5000000,      # Above volume threshold
            'price': 0.0001             # Below custom price threshold
        }
        
        result = self.filter.apply_filters(low_price_data)
        self.assertFalse(result)  # Should fail due to custom price filter


if __name__ == '__main__':
    unittest.main()
