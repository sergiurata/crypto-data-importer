"""
Data Filter for Crypto Importer
Handles filtering and validation of cryptocurrency data
"""

from typing import Dict, List, Optional, Callable
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class FilterRule:
    """Represents a single filter rule"""
    name: str
    filter_func: Callable[[Dict], bool]
    description: str
    enabled: bool = True


class DataFilter:
    """Handles filtering of cryptocurrency data based on configuration"""
    
    def __init__(self, config):
        self.config = config
        self.filters = []
        self._setup_default_filters()
    
    def _setup_default_filters(self):
        """Setup default filters based on configuration"""
        
        # Market cap filter
        min_market_cap = self.config.getfloat('FILTERING', 'min_market_cap', 0)
        if min_market_cap > 0:
            self.add_filter(FilterRule(
                name="market_cap",
                filter_func=lambda coin_data: self.validate_market_cap(
                    coin_data.get('market_cap', 0), min_market_cap
                ),
                description=f"Minimum market cap: ${min_market_cap:,.0f}"
            ))
        
        # Volume filter
        min_volume = self.config.getfloat('FILTERING', 'min_volume_24h', 0)
        if min_volume > 0:
            self.add_filter(FilterRule(
                name="volume_24h",
                filter_func=lambda coin_data: self.validate_volume(
                    coin_data.get('volume_24h', 0), min_volume
                ),
                description=f"Minimum 24h volume: ${min_volume:,.0f}"
            ))
        
        # Symbol exclusion filter
        excluded_symbols = self.config.getlist('FILTERING', 'excluded_symbols')
        if excluded_symbols:
            self.add_filter(FilterRule(
                name="excluded_symbols",
                filter_func=lambda coin_data: self.check_excluded_symbols(
                    coin_data.get('symbol', ''), excluded_symbols
                ),
                description=f"Excluded symbols: {', '.join(excluded_symbols)}"
            ))
        
        # Symbol inclusion filter
        included_symbols = self.config.getlist('FILTERING', 'included_symbols')
        if included_symbols:
            self.add_filter(FilterRule(
                name="included_symbols",
                filter_func=lambda coin_data: self.check_included_symbols(
                    coin_data.get('symbol', ''), included_symbols
                ),
                description=f"Included symbols only: {', '.join(included_symbols)}"
            ))
        
        # Stablecoin exclusion filter
        if self.config.getboolean('FILTERING', 'exclude_stablecoins', False):
            self.add_filter(FilterRule(
                name="exclude_stablecoins",
                filter_func=lambda coin_data: self.exclude_stablecoins(
                    coin_data.get('symbol', ''), coin_data.get('name', '')
                ),
                description="Exclude stablecoins"
            ))
    
    def apply_filters(self, coin_data: Dict) -> bool:
        """Apply all enabled filters to coin data
        
        Args:
            coin_data: Dictionary containing coin information
            
        Returns:
            True if coin passes all filters, False otherwise
        """
        for filter_rule in self.filters:
            if not filter_rule.enabled:
                continue
            
            try:
                if not filter_rule.filter_func(coin_data):
                    logger.debug(f"Coin {coin_data.get('symbol', 'unknown')} filtered out by: {filter_rule.name}")
                    return False
            except Exception as e:
                logger.warning(f"Filter {filter_rule.name} failed for {coin_data.get('symbol', 'unknown')}: {e}")
                return False
        
        return True
    
    def add_filter(self, filter_rule: FilterRule):
        """Add a new filter rule
        
        Args:
            filter_rule: FilterRule object to add
        """
        # Remove existing filter with same name
        self.filters = [f for f in self.filters if f.name != filter_rule.name]
        self.filters.append(filter_rule)
        logger.debug(f"Added filter: {filter_rule.name} - {filter_rule.description}")
    
    def remove_filter(self, filter_name: str):
        """Remove a filter by name
        
        Args:
            filter_name: Name of the filter to remove
        """
        original_count = len(self.filters)
        self.filters = [f for f in self.filters if f.name != filter_name]
        
        if len(self.filters) < original_count:
            logger.debug(f"Removed filter: {filter_name}")
        else:
            logger.warning(f"Filter not found: {filter_name}")
    
    def enable_filter(self, filter_name: str):
        """Enable a filter by name"""
        for filter_rule in self.filters:
            if filter_rule.name == filter_name:
                filter_rule.enabled = True
                logger.debug(f"Enabled filter: {filter_name}")
                return
        logger.warning(f"Filter not found: {filter_name}")
    
    def disable_filter(self, filter_name: str):
        """Disable a filter by name"""
        for filter_rule in self.filters:
            if filter_rule.name == filter_name:
                filter_rule.enabled = False
                logger.debug(f"Disabled filter: {filter_name}")
                return
        logger.warning(f"Filter not found: {filter_name}")
    
    def validate_market_cap(self, market_cap: float, min_value: float) -> bool:
        """Validate market cap against minimum threshold
        
        Args:
            market_cap: Market cap value to validate
            min_value: Minimum required market cap
            
        Returns:
            True if market cap meets minimum, False otherwise
        """
        return market_cap >= min_value
    
    def validate_volume(self, volume: float, min_value: float) -> bool:
        """Validate trading volume against minimum threshold
        
        Args:
            volume: Volume value to validate
            min_value: Minimum required volume
            
        Returns:
            True if volume meets minimum, False otherwise
        """
        return volume >= min_value
    
    def check_excluded_symbols(self, symbol: str, excluded_list: List[str]) -> bool:
        """Check if symbol is in exclusion list
        
        Args:
            symbol: Symbol to check
            excluded_list: List of excluded symbols
            
        Returns:
            True if symbol is NOT excluded, False if excluded
        """
        return symbol.upper() not in [s.upper() for s in excluded_list]
    
    def check_included_symbols(self, symbol: str, included_list: List[str]) -> bool:
        """Check if symbol is in inclusion list
        
        Args:
            symbol: Symbol to check
            included_list: List of included symbols
            
        Returns:
            True if symbol is included, False otherwise
        """
        if not included_list:
            return True  # No inclusion filter means all are included
        
        return symbol.upper() in [s.upper() for s in included_list]
    
    def exclude_stablecoins(self, symbol: str, name: str) -> bool:
        """Check if coin should be excluded as a stablecoin
        
        Args:
            symbol: Coin symbol
            name: Coin name
            
        Returns:
            True if NOT a stablecoin, False if is a stablecoin
        """
        stablecoin_indicators = [
            'usd', 'usdt', 'usdc', 'dai', 'busd', 'tusd', 'usdn', 'fei',
            'frax', 'lusd', 'susd', 'gusd', 'paxg', 'ustc', 'terra',
            'stablecoin', 'stable', 'dollar', 'euro', 'eur'
        ]
        
        symbol_lower = symbol.lower()
        name_lower = name.lower()
        
        # Check if any stablecoin indicator is in symbol or name
        for indicator in stablecoin_indicators:
            if indicator in symbol_lower or indicator in name_lower:
                return False  # It's a stablecoin, exclude it
        
        return True  # Not a stablecoin, include it
    
    def validate_price_range(self, price: float, min_price: float = 0, max_price: float = float('inf')) -> bool:
        """Validate price is within specified range
        
        Args:
            price: Price to validate
            min_price: Minimum price threshold
            max_price: Maximum price threshold
            
        Returns:
            True if price is within range, False otherwise
        """
        return min_price <= price <= max_price
    
    def validate_age(self, launch_date: str, min_days: int = 0) -> bool:
        """Validate coin age against minimum days
        
        Args:
            launch_date: Launch date string (ISO format)
            min_days: Minimum age in days
            
        Returns:
            True if coin is old enough, False otherwise
        """
        try:
            from datetime import datetime, timedelta
            
            if not launch_date:
                return True  # No date info, assume it's old enough
            
            launch = datetime.fromisoformat(launch_date.replace('Z', '+00:00'))
            