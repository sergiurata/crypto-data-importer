"""
Factory Classes for Dynamic Component Creation
Implements the Factory Pattern for extensible component creation
"""

from typing import Dict, Type, List, Optional
import logging
import importlib

from abstract_data_provider import AbstractDataProvider
from abstract_exchange_mapper import AbstractExchangeMapper
from abstract_database_adapter import AbstractDatabaseAdapter

logger = logging.getLogger(__name__)


class ProviderFactory:
    """Factory for creating data provider instances"""
    
    _providers: Dict[str, Type[AbstractDataProvider]] = {}
    
    @classmethod
    def register_provider(cls, name: str, provider_class: Type[AbstractDataProvider]):
        """Register a data provider class
        
        Args:
            name: Name to register the provider under
            provider_class: Provider class to register
        """
        cls._providers[name.lower()] = provider_class
        logger.debug(f"Registered data provider: {name}")
    
    @classmethod
    def create_data_provider(cls, provider_type: str, config) -> Optional[AbstractDataProvider]:
        """Create a data provider instance
        
        Args:
            provider_type: Type of provider to create
            config: Configuration object
            
        Returns:
            Data provider instance or None if not found
        """
        provider_class = cls._providers.get(provider_type.lower())
        if provider_class:
            try:
                return provider_class(config)
            except Exception as e:
                logger.error(f"Failed to create data provider {provider_type}: {e}")
                return None
        else:
            logger.error(f"Unknown data provider type: {provider_type}")
            return None
    
    @classmethod
    def get_available_providers(cls) -> List[str]:
        """Get list of available provider types
        
        Returns:
            List of registered provider names
        """
        return list(cls._providers.keys())
    
    @classmethod
    def load_provider_from_module(cls, name: str, module_path: str, class_name: str) -> bool:
        """Dynamically load a provider from a module
        
        Args:
            name: Name to register the provider under
            module_path: Path to the module
            class_name: Name of the class in the module
            
        Returns:
            True if loaded successfully, False otherwise
        """
        try:
            module = importlib.import_module(module_path)
            provider_class = getattr(module, class_name)
            
            if issubclass(provider_class, AbstractDataProvider):
                cls.register_provider(name, provider_class)
                return True
            else:
                logger.error(f"Class {class_name} is not a subclass of AbstractDataProvider")
                return False
                
        except Exception as e:
            logger.error(f"Failed to load provider from {module_path}.{class_name}: {e}")
            return False


class MapperFactory:
    """Factory for creating exchange mapper instances"""
    
    _mappers: Dict[str, Type[AbstractExchangeMapper]] = {}
    
    @classmethod
    def register_mapper(cls, name: str, mapper_class: Type[AbstractExchangeMapper]):
        """Register an exchange mapper class
        
        Args:
            name: Name to register the mapper under
            mapper_class: Mapper class to register
        """
        cls._mappers[name.lower()] = mapper_class
        logger.debug(f"Registered exchange mapper: {name}")
    
    @classmethod
    def create_exchange_mapper(cls, exchange_name: str, config) -> Optional[AbstractExchangeMapper]:
        """Create an exchange mapper instance
        
        Args:
            exchange_name: Name of exchange mapper to create
            config: Configuration object
            
        Returns:
            Exchange mapper instance or None if not found
        """
        mapper_class = cls._mappers.get(exchange_name.lower())
        if mapper_class:
            try:
                return mapper_class(config)
            except Exception as e:
                logger.error(f"Failed to create exchange mapper {exchange_name}: {e}")
                return None
        else:
            logger.error(f"Unknown exchange mapper: {exchange_name}")
            return None
    
    @classmethod
    def create_multiple_mappers(cls, exchange_names: List[str], config) -> List[AbstractExchangeMapper]:
        """Create multiple exchange mapper instances
        
        Args:
            exchange_names: List of exchange names
            config: Configuration object
            
        Returns:
            List of successfully created mapper instances
        """
        mappers = []
        for exchange_name in exchange_names:
            mapper = cls.create_exchange_mapper(exchange_name, config)
            if mapper:
                mappers.append(mapper)
            else:
                logger.warning(f"Failed to create mapper for {exchange_name}")
        
        return mappers
    
    @classmethod
    def get_available_exchanges(cls) -> List[str]:
        """Get list of available exchange types
        
        Returns:
            List of registered exchange names
        """
        return list(cls._mappers.keys())
    
    @classmethod
    def load_mapper_from_module(cls, name: str, module_path: str, class_name: str) -> bool:
        """Dynamically load a mapper from a module
        
        Args:
            name: Name to register the mapper under
            module_path: Path to the module
            class_name: Name of the class in the module
            
        Returns:
            True if loaded successfully, False otherwise
        """
        try:
            module = importlib.import_module(module_path)
            mapper_class = getattr(module, class_name)
            
            if issubclass(mapper_class, AbstractExchangeMapper):
                cls.register_mapper(name, mapper_class)
                return True
            else:
                logger.error(f"Class {class_name} is not a subclass of AbstractExchangeMapper")
                return False
                
        except Exception as e:
            logger.error(f"Failed to load mapper from {module_path}.{class_name}: {e}")
            return False


class AdapterFactory:
    """Factory for creating database adapter instances"""
    
    _adapters: Dict[str, Type[AbstractDatabaseAdapter]] = {}
    
    @classmethod
    def register_adapter(cls, name: str, adapter_class: Type[AbstractDatabaseAdapter]):
        """Register a database adapter class
        
        Args:
            name: Name to register the adapter under
            adapter_class: Adapter class to register
        """
        cls._adapters[name.lower()] = adapter_class
        logger.debug(f"Registered database adapter: {name}")
    
    @classmethod
    def create_database_adapter(cls, adapter_type: str, config) -> Optional[AbstractDatabaseAdapter]:
        """Create a database adapter instance
        
        Args:
            adapter_type: Type of adapter to create
            config: Configuration object
            
        Returns:
            Database adapter instance or None if not found
        """
        adapter_class = cls._adapters.get(adapter_type.lower())
        if adapter_class:
            try:
                adapter = adapter_class(config)
                
                # Connect to database
                database_path = config.get('DATABASE', 'database_path')
                if database_path:
                    if not adapter.connect(database_path):
                        logger.error(f"Failed to connect to database: {database_path}")
                        return None
                
                return adapter
            except Exception as e:
                logger.error(f"Failed to create database adapter {adapter_type}: {e}")
                return None
        else:
            logger.error(f"Unknown database adapter type: {adapter_type}")
            return None
    
    @classmethod
    def get_available_adapters(cls) -> List[str]:
        """Get list of available adapter types
        
        Returns:
            List of registered adapter names
        """
        return list(cls._adapters.keys())
    
    @classmethod
    def load_adapter_from_module(cls, name: str, module_path: str, class_name: str) -> bool:
        """Dynamically load an adapter from a module
        
        Args:
            name: Name to register the adapter under
            module_path: Path to the module
            class_name: Name of the class in the module
            
        Returns:
            True if loaded successfully, False otherwise
        """
        try:
            module = importlib.import_module(module_path)
            adapter_class = getattr(module, class_name)
            
            if issubclass(adapter_class, AbstractDatabaseAdapter):
                cls.register_adapter(name, adapter_class)
                return True
            else:
                logger.error(f"Class {class_name} is not a subclass of AbstractDatabaseAdapter")
                return False
                
        except Exception as e:
            logger.error(f"Failed to load adapter from {module_path}.{class_name}: {e}")
            return False


def register_default_implementations():
    """Register the default implementations with their factories"""
    try:
        # Register CoinGecko provider
        from coingecko_provider import CoinGeckoProvider
        ProviderFactory.register_provider('coingecko', CoinGeckoProvider)
        
        # Register Kraken mapper
        from kraken_mapper import KrakenMapper
        MapperFactory.register_mapper('kraken', KrakenMapper)
        
        # Register AmiBroker adapter
        from amibroker_adapter import AmiBrokerAdapter
        AdapterFactory.register_adapter('amibroker', AmiBrokerAdapter)
        
        logger.info("Registered default implementations")
        
    except ImportError as e:
        logger.error(f"Failed to register default implementations: {e}")


def load_custom_implementations(config):
    """Load custom implementations from configuration
    
    Args:
        config: Configuration manager instance
    """
    try:
        # Load custom providers
        custom_providers = config.getlist('EXTENSIONS', 'custom_providers')
        for provider_config in custom_providers:
            # Format: "name:module_path:class_name"
            parts = provider_config.split(':')
            if len(parts) == 3:
                name, module_path, class_name = parts
                ProviderFactory.load_provider_from_module(name, module_path, class_name)
        
        # Load custom mappers
        custom_mappers = config.getlist('EXTENSIONS', 'custom_mappers')
        for mapper_config in custom_mappers:
            parts = mapper_config.split(':')
            if len(parts) == 3:
                name, module_path, class_name = parts
                MapperFactory.load_mapper_from_module(name, module_path, class_name)
        
        # Load custom adapters
        custom_adapters = config.getlist('EXTENSIONS', 'custom_adapters')
        for adapter_config in custom_adapters:
            parts = adapter_config.split(':')
            if len(parts) == 3:
                name, module_path, class_name = parts
                AdapterFactory.load_adapter_from_module(name, module_path, class_name)
        
        logger.info("Loaded custom implementations from configuration")
        
    except Exception as e:
        logger.debug(f"No custom implementations configured or failed to load: {e}")


def create_components_from_config(config):
    """Create all components based on configuration
    
    Args:
        config: Configuration manager instance
        
    Returns:
        Tuple of (data_provider, exchange_mappers, database_adapter)
    """
    # Ensure default implementations are registered
    register_default_implementations()
    
    # Load any custom implementations
    load_custom_implementations(config)
    
    # Create data provider
    provider_type = config.get('PROVIDERS', 'data_provider', 'coingecko')
    data_provider = ProviderFactory.create_data_provider(provider_type, config)
    
    if not data_provider:
        raise ValueError(f"Failed to create data provider: {provider_type}")
    
    # Create exchange mappers
    exchange_names = config.getlist('PROVIDERS', 'exchanges')
    if not exchange_names:
        exchange_names = ['kraken']  # Default to Kraken
    
    exchange_mappers = MapperFactory.create_multiple_mappers(exchange_names, config)
    
    if not exchange_mappers:
        logger.warning("No exchange mappers created")
    
    # Create database adapter
    adapter_type = config.get('PROVIDERS', 'database_adapter', 'amibroker')
    database_adapter = AdapterFactory.create_database_adapter(adapter_type, config)
    
    if not database_adapter:
        raise ValueError(f"Failed to create database adapter: {adapter_type}")
    
    return data_provider, exchange_mappers, database_adapter


def get_factory_status() -> Dict:
    """Get status of all factories
    
    Returns:
        Dictionary containing factory status information
    """
    return {
        'providers': {
            'available': ProviderFactory.get_available_providers(),
            'count': len(ProviderFactory.get_available_providers())
        },
        'exchanges': {
            'available': MapperFactory.get_available_exchanges(),
            'count': len(MapperFactory.get_available_exchanges())
        },
        'adapters': {
            'available': AdapterFactory.get_available_adapters(),
            'count': len(AdapterFactory.get_available_adapters())
        }
    }
