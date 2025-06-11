"""
Abstract Exchange Mapper Base Class
Defines the interface for mapping coins to exchange-specific data
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from dataclasses import dataclass
import logging

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
    
    def build_mapping(self) -> Dict:
        """Build the mapping between CoinGecko IDs and exchange data
        
        Returns:
            Dictionary containing the mapping
        """
        logger.info(f"Building mapping for {self.exchange_name}")
        
        if not self.load_exchange_data():
            logger.error(f"Failed to load exchange data for {self.exchange_name}")
            return {}
        
        mapping = self._build_coin_mapping()
        logger.info(f"Built mapping for {len(mapping)} coins on {self.exchange_name}")
        
        return mapping
    
    @abstractmethod
    def _build_coin_mapping(self) -> Dict:
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
