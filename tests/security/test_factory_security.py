"""
Security tests for factory classes - CVE-001 validation
These tests verify that the arbitrary code execution vulnerability has been fixed
"""

import unittest
import os
import sys
import logging
from unittest.mock import patch, MagicMock
from pathlib import Path

# Add src to path for testing
src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from core.factory_classes import (
    ProviderFactory, 
    MapperFactory, 
    AdapterFactory, 
    ModuleSecurityValidator,
    load_custom_implementations,
    ALLOW_DYNAMIC_LOADING
)


class TestFactorySecurityCVE001(unittest.TestCase):
    """Test CVE-001 fixes - Arbitrary Code Execution Prevention"""
    
    def setUp(self):
        """Set up test environment"""
        # Ensure dynamic loading is disabled for security tests
        self.original_allow_loading = os.getenv('CRYPTO_ALLOW_DYNAMIC_LOADING')
        if 'CRYPTO_ALLOW_DYNAMIC_LOADING' in os.environ:
            del os.environ['CRYPTO_ALLOW_DYNAMIC_LOADING']
        
        # Reload module to get updated ALLOW_DYNAMIC_LOADING value
        import importlib
        import core.factory_classes
        importlib.reload(core.factory_classes)
        
        # Set up logging to capture security messages
        self.log_messages = []
        self.log_handler = logging.Handler()
        self.log_handler.emit = lambda record: self.log_messages.append(record.getMessage())
        
        factory_logger = logging.getLogger('core.factory_classes')
        factory_logger.addHandler(self.log_handler)
        factory_logger.setLevel(logging.DEBUG)
    
    def tearDown(self):
        """Clean up test environment"""
        # Restore original environment
        if self.original_allow_loading is not None:
            os.environ['CRYPTO_ALLOW_DYNAMIC_LOADING'] = self.original_allow_loading
        
        # Remove log handler
        factory_logger = logging.getLogger('core.factory_classes')
        factory_logger.removeHandler(self.log_handler)
    
    def test_dynamic_loading_disabled_by_default(self):
        """Test that dynamic loading is disabled by default"""
        # Verify ALLOW_DYNAMIC_LOADING is False
        from core.factory_classes import ALLOW_DYNAMIC_LOADING
        self.assertFalse(ALLOW_DYNAMIC_LOADING, "Dynamic loading should be disabled by default")
    
    def test_blocks_arbitrary_code_execution_provider(self):
        """Test that arbitrary code execution is prevented in ProviderFactory"""
        # Attempt to load dangerous modules
        malicious_paths = [
            'os',
            'subprocess', 
            'sys',
            '__builtins__',
            'importlib',
            '../../../etc/passwd',
            'eval',
            'exec',
        ]
        
        for path in malicious_paths:
            with self.subTest(path=path):
                result = ProviderFactory.load_provider_from_module('test', path, 'TestClass')
                self.assertFalse(result, f"Should block loading from dangerous path: {path}")
                
                # Check that security warning was logged
                security_messages = [msg for msg in self.log_messages if 'SECURITY' in msg]
                self.assertTrue(len(security_messages) > 0, "Security warning should be logged")
    
    def test_blocks_arbitrary_code_execution_mapper(self):
        """Test that arbitrary code execution is prevented in MapperFactory"""
        # Attempt to load dangerous modules
        malicious_paths = [
            'os.system',
            'subprocess.call',
            'sys.modules',
            '../../../bin/bash',
            'importlib.import_module',
        ]
        
        for path in malicious_paths:
            with self.subTest(path=path):
                result = MapperFactory.load_mapper_from_module('test', path, 'TestClass')
                self.assertFalse(result, f"Should block loading from dangerous path: {path}")
    
    def test_blocks_arbitrary_code_execution_adapter(self):
        """Test that arbitrary code execution is prevented in AdapterFactory"""
        # Attempt to load dangerous modules
        malicious_paths = [
            'builtins',
            'types',
            '../../..',
            'os.path.join',
            'eval("print(1)")',
        ]
        
        for path in malicious_paths:
            with self.subTest(path=path):
                result = AdapterFactory.load_adapter_from_module('test', path, 'TestClass')
                self.assertFalse(result, f"Should block loading from dangerous path: {path}")
    
    def test_prevents_directory_traversal(self):
        """Test that directory traversal attacks are prevented"""
        traversal_patterns = [
            '../../../etc/passwd',
            '..\\..\\..\\windows\\system32',
            '/etc/passwd',
            'C:\\Windows\\System32',
            '../../../../../../usr/bin/python',
            '../malicious_module',
            '..\\malicious_module',
        ]
        
        for pattern in traversal_patterns:
            with self.subTest(pattern=pattern):
                result = ProviderFactory.load_provider_from_module('test', pattern, 'TestClass')
                self.assertFalse(result, f"Should block directory traversal: {pattern}")
    
    def test_validates_module_paths(self):
        """Test module path validation"""
        # Valid paths (should still be blocked because dynamic loading is disabled)
        valid_paths = [
            'providers.test_provider',
            'mappers.test_mapper',
            'adapters.test_adapter',
        ]
        
        for path in valid_paths:
            with self.subTest(path=path):
                # Even valid paths should be blocked when dynamic loading is disabled
                result = ProviderFactory.load_provider_from_module('test', path, 'TestClass')
                self.assertFalse(result, f"Should block even valid paths when dynamic loading disabled: {path}")
    
    def test_validates_class_names(self):
        """Test class name validation"""
        dangerous_class_names = [
            '__import__',
            'eval',
            'exec',
            'compile',
            'open',
            'input',
            'raw_input',
            '__builtins__',
            'os.system',
        ]
        
        for class_name in dangerous_class_names:
            with self.subTest(class_name=class_name):
                result = ProviderFactory.load_provider_from_module('test', 'providers.test', class_name)
                self.assertFalse(result, f"Should block dangerous class name: {class_name}")
    
    def test_configuration_injection_prevention(self):
        """Test that configuration injection is prevented"""
        # Mock configuration with malicious entries
        mock_config = MagicMock()
        mock_config.getlist.return_value = [
            'malicious:os:system',
            'evil:subprocess:call',
            'bad:../../etc:passwd',
            'danger:sys:modules',
        ]
        
        # Should not load any custom implementations when disabled
        load_custom_implementations(mock_config)
        
        # Verify security message was logged
        security_messages = [msg for msg in self.log_messages if 'Custom implementations disabled for security' in msg]
        self.assertTrue(len(security_messages) > 0, "Should log security message about disabled custom implementations")
    
    def test_module_security_validator(self):
        """Test the ModuleSecurityValidator class"""
        validator = ModuleSecurityValidator()
        
        # Test invalid module paths
        invalid_paths = [
            '',
            None,
            '../../../etc/passwd',
            'os.system',
            'subprocess.call',
            'sys.modules',
            '/absolute/path',
            'C:\\Windows\\System32',
            'very_long_module_path_' + 'x' * 100,  # Too long
            'invalid-characters!@#',
            'providers.__builtins__',
        ]
        
        for path in invalid_paths:
            with self.subTest(path=path):
                self.assertFalse(
                    validator.validate_module_path(path), 
                    f"Should reject invalid module path: {path}"
                )
        
        # Test valid module paths
        valid_paths = [
            'providers.coingecko_provider',
            'mappers.kraken_mapper',
            'adapters.amibroker_adapter',
        ]
        
        for path in valid_paths:
            with self.subTest(path=path):
                self.assertTrue(
                    validator.validate_module_path(path), 
                    f"Should accept valid module path: {path}"
                )
    
    def test_class_name_validation(self):
        """Test class name validation"""
        validator = ModuleSecurityValidator()
        
        # Test invalid class names
        invalid_names = [
            '',
            None,
            '__import__',
            'eval',
            'exec',
            'compile',
            'open',
            'lowercase_start',  # Should start with capital
            'Invalid-Characters',
            'Very_Long_Class_Name_' + 'X' * 50,  # Too long
            'Class__With__Dunder',
        ]
        
        for name in invalid_names:
            with self.subTest(name=name):
                self.assertFalse(
                    validator.validate_class_name(name), 
                    f"Should reject invalid class name: {name}"
                )
        
        # Test valid class names
        valid_names = [
            'CoinGeckoProvider',
            'KrakenMapper',
            'AmiBrokerAdapter',
            'TestClass123',
            'MyCustomProvider',
        ]
        
        for name in valid_names:
            with self.subTest(name=name):
                self.assertTrue(
                    validator.validate_class_name(name), 
                    f"Should accept valid class name: {name}"
                )
    
    def test_environment_variable_control(self):
        """Test that environment variable properly controls dynamic loading"""
        # Test with dynamic loading explicitly disabled
        with patch.dict(os.environ, {'CRYPTO_ALLOW_DYNAMIC_LOADING': 'false'}):
            # Reload module to get updated value
            import importlib
            import core.factory_classes
            importlib.reload(core.factory_classes)
            
            from core.factory_classes import ALLOW_DYNAMIC_LOADING
            self.assertFalse(ALLOW_DYNAMIC_LOADING)
        
        # Test with dynamic loading explicitly enabled (should still validate)
        with patch.dict(os.environ, {'CRYPTO_ALLOW_DYNAMIC_LOADING': 'true'}):
            # Reload module to get updated value
            import importlib
            import core.factory_classes
            importlib.reload(core.factory_classes)
            
            from core.factory_classes import ALLOW_DYNAMIC_LOADING
            self.assertTrue(ALLOW_DYNAMIC_LOADING)
    
    def test_security_logging(self):
        """Test that security events are properly logged"""
        # Attempt a malicious operation
        ProviderFactory.load_provider_from_module('test', 'os', 'system')
        
        # Check that appropriate security messages were logged
        security_messages = [msg for msg in self.log_messages if 'SECURITY' in msg]
        self.assertTrue(len(security_messages) >= 1, "Security events should be logged")
        
        # Check for specific security messages
        disabled_messages = [msg for msg in self.log_messages if 'Dynamic module loading is disabled' in msg]
        self.assertTrue(len(disabled_messages) >= 1, "Should log that dynamic loading is disabled")
        
        blocked_messages = [msg for msg in self.log_messages if 'Blocked attempt' in msg]
        self.assertTrue(len(blocked_messages) >= 1, "Should log blocked attempts")


class TestFactorySecurityIntegration(unittest.TestCase):
    """Integration tests for factory security"""
    
    def test_normal_operations_still_work(self):
        """Test that normal factory operations still work with security enabled"""
        # Test that default implementations can still be registered
        from core.factory_classes import register_default_implementations
        
        # This should work fine (uses static imports)
        register_default_implementations()
        
        # Verify providers are registered
        available_providers = ProviderFactory.get_available_providers()
        self.assertIn('coingecko', available_providers)
        
        # Verify mappers are registered
        available_mappers = MapperFactory.get_available_exchanges()
        self.assertIn('kraken', available_mappers)
        
        # Verify adapters are registered
        available_adapters = AdapterFactory.get_available_adapters()
        self.assertIn('amibroker', available_adapters)
    
    def test_component_creation_works(self):
        """Test that component creation still works normally"""
        # Mock configuration
        mock_config = MagicMock()
        mock_config.get.side_effect = lambda section, key, default=None: {
            ('DATABASE', 'database_path'): '/test/path.db'
        }.get((section, key), default)
        
        # Register default implementations first
        from core.factory_classes import register_default_implementations
        register_default_implementations()
        
        # Test provider creation
        provider = ProviderFactory.create_data_provider('coingecko', mock_config)
        self.assertIsNotNone(provider, "Should be able to create providers normally")
        
        # Test mapper creation
        mapper = MapperFactory.create_exchange_mapper('kraken', mock_config)
        self.assertIsNotNone(mapper, "Should be able to create mappers normally")


if __name__ == '__main__':
    # Run the security tests
    unittest.main(verbosity=2)