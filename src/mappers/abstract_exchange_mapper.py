"""
Abstract Exchange Mapper Base Class
Defines the interface for mapping coins to exchange-specific data
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from dataclasses import dataclass
import logging
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from providers.abstract_data_provider import AbstractDataProvider

logger = logging.getLogger(__name__)

@dataclass
class ExchangeInfo:
    """Information about a coin on a specific exchange"""
    exchange_name: str
    symbol: str
    pair_name: str
    base_currency: str
    target_currency: str
    alt_name: str = ""
    trade_url: str = ""
    is_active: bool = True
    min_order_size: float = 0.0
    fee_percent: float = 0.0


class AbstractExchangeMapper(ABC):
    """Abstract base class for exchange mapping implementations"""
    
    def __init__(self, config):
        self.config = config
        self.exchange_name = self.get_exchange_name()
        self.mapping_cache = {}
        self.last_update = None
        
        # Add rate limiting configuration
        self.requests_per_minute = config.getint('API', 'requests_per_minute', 40)
        self.rate_limit_delay = 60.0 / self.requests_per_minute  # Calculate delay between requests
        self.last_request_time = 0
        self.timeout = config.getint('API', 'timeout_seconds', 30)
        self.retry_attempts = config.getint('API', 'retry_attempts', 3)
    
    @abstractmethod
    def get_exchange_name(self) -> str:
        """Get the name of this exchange
        
        Returns:
            String name of the exchange
        """
        pass
    
    @abstractmethod
    def load_exchange_data(self) -> bool:
        """Load exchange-specific data (assets, pairs, etc.)
        
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def map_coin_to_exchange(self, coin_id: str) -> Optional[ExchangeInfo]:
        """Map a CoinGecko coin ID to exchange-specific information
        
        Args:
            coin_id: CoinGecko coin identifier
            
        Returns:
            ExchangeInfo object if mapping found, None otherwise
        """
        pass
    
    @abstractmethod
    def is_tradeable(self, coin_id: str) -> bool:
        """Check if a coin is tradeable on this exchange
        
        Args:
            coin_id: CoinGecko coin identifier
            
        Returns:
            True if tradeable, False otherwise
        """
        pass
    
    @abstractmethod
    def get_symbol_mapping(self, coin_id: str) -> Optional[str]:
        """Get exchange-specific symbol for a coin
        
        Args:
            coin_id: CoinGecko coin identifier
            
        Returns:
            Exchange symbol if found, None otherwise
        """
        pass
    
    def build_mapping(self, data_provider: AbstractDataProvider) -> Dict:
        """Build the mapping between CoinGecko IDs and exchange data
        
        Returns:
            Dictionary containing the mapping
        """
        logger.info(f"Building mapping for {self.exchange_name}")
        
        if not self.load_exchange_data():
            logger.error(f"Failed to load exchange data for {self.exchange_name}")
            return {}
        
        mapping = self._build_coin_mapping(data_provider)
        logger.info(f"Built mapping for {len(mapping)} coins on {self.exchange_name}")
        
        return mapping
    
    @abstractmethod
    def _build_coin_mapping(self, data_provider: AbstractDataProvider) -> Dict:
        """Implementation-specific mapping building logic
        
        Returns:
            Dictionary containing coin mappings
        """
        pass
    
    def get_all_tradeable_symbols(self) -> List[str]:
        """Get all symbols tradeable on this exchange
        
        Returns:
            List of tradeable symbols
        """
        if not self.mapping_cache:
            self.build_mapping()
        
        return list(self.mapping_cache.keys())
    
    def get_exchange_info(self, coin_id: str) -> Optional[ExchangeInfo]:
        """Get comprehensive exchange information for a coin
        
        Args:
            coin_id: CoinGecko coin identifier
            
        Returns:
            ExchangeInfo object with all available data
        """
        return self.map_coin_to_exchange(coin_id)
    
    def validate_mapping(self) -> bool:
        """Validate the current mapping data
        
        Returns:
            True if mapping is valid, False otherwise
        """
        if not self.mapping_cache:
            logger.warning(f"No mapping data available for {self.exchange_name}")
            return False
        
        # Basic validation - check if mapping contains expected data
        sample_key = next(iter(self.mapping_cache), None)
        if sample_key:
            sample_data = self.mapping_cache[sample_key]
            required_fields = ['exchange_name', 'symbol', 'pair_name']
            
            if isinstance(sample_data, dict):
                missing_fields = [field for field in required_fields if field not in sample_data]
                if missing_fields:
                    logger.warning(f"Mapping missing required fields: {missing_fields}")
                    return False
        
        logger.info(f"Mapping validation successful for {self.exchange_name}")
        return True
    
    def refresh_mapping(self) -> bool:
        """Refresh the mapping data from the exchange
        
        Returns:
            True if refresh successful, False otherwise
        """
        logger.info(f"Refreshing mapping for {self.exchange_name}")
        
        try:
            self.mapping_cache.clear()
            new_mapping = self.build_mapping()
            
            if new_mapping:
                self.mapping_cache = new_mapping
                return self.validate_mapping()
            else:
                logger.error(f"Failed to refresh mapping for {self.exchange_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error refreshing mapping for {self.exchange_name}: {e}")
            return False
    
    def get_mapping_stats(self) -> Dict:
        """Get statistics about the current mapping
        
        Returns:
            Dictionary containing mapping statistics
        """
        if not self.mapping_cache:
            return {'total_coins': 0, 'exchange': self.exchange_name}
        
        stats = {
            'exchange': self.exchange_name,
            'total_coins': len(self.mapping_cache),
            'last_update': self.last_update
        }
        
        # Count by target currency if available
        target_counts = {}
        for coin_data in self.mapping_cache.values():
            if isinstance(coin_data, dict):
                target = coin_data.get('target_currency', 'Unknown')
            elif hasattr(coin_data, 'target_currency'):
                target = coin_data.target_currency
            else:
                target = 'Unknown'
                
            target_counts[target] = target_counts.get(target, 0) + 1
        
        stats['target_currencies'] = target_counts
        return stats
    
    def supports_coin(self, coin_id: str) -> bool:
        """Check if this exchange supports a specific coin
        
        Args:
            coin_id: CoinGecko coin identifier
            
        Returns:
            True if supported, False otherwise
        """
        return self.is_tradeable(coin_id)
    
    def get_trading_pairs(self, base_currency: str = None) -> List[ExchangeInfo]:
        """Get all trading pairs, optionally filtered by base currency
        
        Args:
            base_currency: Optional filter by base currency
            
        Returns:
            List of ExchangeInfo objects for trading pairs
        """
        pairs = []
        
        for coin_id in self.mapping_cache:
            exchange_info = self.map_coin_to_exchange(coin_id)
            if exchange_info:
                if base_currency is None or exchange_info.base_currency == base_currency:
                    pairs.append(exchange_info)
        
        return pairs
    
    def handle_rate_limiting(self):
        """Handle rate limiting between API requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def make_api_request(self, url: str, **kwargs):
        for attempt in range(self.retry_attempts):
            self.handle_rate_limiting()
            try:
                response = requests.get(url, timeout=self.timeout, **kwargs)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt < self.retry_attempts - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"All retry attempts failed: {e}")
                    raise        