"""
CoinGecko Data Provider Implementation
Handles all CoinGecko API interactions
"""

import pandas as pd
import time
import json
import os
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import logging

from .abstract_data_provider import AbstractDataProvider

logger = logging.getLogger(__name__)


class CoinGeckoProvider(AbstractDataProvider):
    """CoinGecko API implementation of AbstractDataProvider"""
    
    def __init__(self, config):
        super().__init__(config)
        self.base_url = "https://api.coingecko.com/api/v3"
        
        # Set API key if provided
        api_key = config.get('API', 'coingecko_api_key')
        if api_key:
            self.session.headers.update({'x-cg-demo-api-key': api_key})
            logger.info("Using CoinGecko API key")
        
        # Update headers
        headers = self.get_default_headers()
        headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.session.headers.update(headers)
        
        # Initialize caching system
        self.cache_enabled = config.getboolean('CACHE', 'enable_api_cache', True)
        self.cache_file = config.get('CACHE', 'cache_file', 'coingecko_api_cache.json')
        self.exchange_data_ttl_hours = config.getint('CACHE', 'exchange_data_ttl_hours', 24)
        self.market_data_ttl_hours = config.getint('CACHE', 'market_data_ttl_hours', 1)
        self.coin_details_ttl_hours = config.getint('CACHE', 'coin_details_ttl_hours', 6)
        self.max_cache_size_mb = config.getint('CACHE', 'max_cache_size_mb', 100)
        self.clear_expired_on_startup = config.getboolean('CACHE', 'clear_expired_on_startup', True)
        
        # Load existing cache
        self.api_cache = {}
        if self.cache_enabled:
            self._load_cache()
            if self.clear_expired_on_startup:
                self._clear_expired_entries()
            logger.info(f"API caching enabled - {len(self.api_cache)} entries in cache")
    
    def get_all_coins(self) -> List[Dict]:
        """Get list of all coins from CoinGecko"""
        def _make_request():
            url = f"{self.base_url}/coins/list"
            response = self.session.get(url, timeout=self.timeout)
            
            if not self.validate_response(response):
                return None
                
            return response.json()
        
        start_time = time.time()
        result = self.retry_request(_make_request)
        response_time = time.time() - start_time
        
        if result:
            logger.info(f"Retrieved {len(result)} coins from CoinGecko")
            self.log_api_usage("coins/list", "success", response_time)
            return result
        else:
            logger.error("Failed to retrieve coins list from CoinGecko")
            self.log_api_usage("coins/list", "failure", response_time)
            return []
    
    def get_market_data(self, coin_id: str, days: int = 365) -> Optional[Dict]:
        """Get historical market data for a specific coin"""
        # Check cache first
        params = {
            'vs_currency': 'usd',
            'days': days,
            'interval': 'daily'
        }
        cache_key = self._get_cache_key(f"coins/{coin_id}/market_chart", params)
        cached_result = self._get_from_cache(cache_key, self.market_data_ttl_hours)
        if cached_result:
            logger.debug(f"Using cached market data for {coin_id} ({days} days)")
            return cached_result
        
        def _make_request():
            url = f"{self.base_url}/coins/{coin_id}/market_chart"
            response = self.session.get(url, params=params, timeout=self.timeout)
            
            if not self.validate_response(response):
                return None
                
            return response.json()
        
        start_time = time.time()
        result = self.retry_request(_make_request)
        response_time = time.time() - start_time
        
        endpoint = f"coins/{coin_id}/market_chart"
        if result:
            # Store in cache
            self._store_in_cache(cache_key, result)
            self._save_cache()
            self.log_api_usage(endpoint, "success", response_time)
            logger.debug(f"Fetched and cached market data for {coin_id} ({days} days)")
            return result
        else:
            logger.error(f"Failed to get market data for {coin_id}")
            self.log_api_usage(endpoint, "failure", response_time)
            return None
    
    def get_exchange_data(self, coin_id: str) -> Optional[Dict]:
        """Get exchange-specific data for a coin"""
        # Check cache first
        params = {
            'tickers': 'true',
            'community_data': 'false',
            'developer_data': 'false',
            'sparkline': 'false'
        }
        cache_key = self._get_cache_key(f"coins/{coin_id}", params)
        cached_result = self._get_from_cache(cache_key, self.exchange_data_ttl_hours)
        if cached_result:
            logger.debug(f"Using cached exchange data for {coin_id}")
            return cached_result
        
        def _make_request():
            url = f"{self.base_url}/coins/{coin_id}"
            response = self.session.get(url, params=params, timeout=self.timeout)
            
            if not self.validate_response(response):
                return None
                
            return response.json()
        
        start_time = time.time()
        result = self.retry_request(_make_request)
        response_time = time.time() - start_time
        
        endpoint = f"coins/{coin_id}"
        if result:
            # Store in cache
            self._store_in_cache(cache_key, result)
            self._save_cache()
            self.log_api_usage(endpoint, "success", response_time)
            logger.debug(f"Fetched and cached exchange data for {coin_id}")
            return result
        else:
            logger.error(f"Failed to get exchange data for {coin_id}")
            self.log_api_usage(endpoint, "failure", response_time)
            return None
    
    def get_coin_details(self, coin_id: str) -> Optional[Dict]:
        """Get detailed information about a specific coin"""
        # Check cache first
        params = {
            'localization': 'false',
            'tickers': 'false',
            'market_data': 'true',
            'community_data': 'false',
            'developer_data': 'false',
            'sparkline': 'false'
        }
        cache_key = self._get_cache_key(f"coins/{coin_id}/details", params)
        cached_result = self._get_from_cache(cache_key, self.coin_details_ttl_hours)
        if cached_result:
            logger.debug(f"Using cached coin details for {coin_id}")
            return cached_result
        
        def _make_request():
            url = f"{self.base_url}/coins/{coin_id}"
            response = self.session.get(url, params=params, timeout=self.timeout)
            
            if not self.validate_response(response):
                return None
                
            return response.json()
        
        start_time = time.time()
        result = self.retry_request(_make_request)
        response_time = time.time() - start_time
        
        endpoint = f"coins/{coin_id}/details"
        if result:
            # Store in cache
            self._store_in_cache(cache_key, result)
            self._save_cache()
            self.log_api_usage(endpoint, "success", response_time)
            logger.debug(f"Fetched and cached coin details for {coin_id}")
            return result
        else:
            logger.error(f"Failed to get coin details for {coin_id}")
            self.log_api_usage(endpoint, "failure", response_time)
            return None
    
    def format_market_data(self, coin_data: Dict, coin_info: Dict) -> Optional[pd.DataFrame]:
        """Format CoinGecko data into a standardized DataFrame"""
        try:
            prices = coin_data.get('prices', [])
            market_caps = coin_data.get('market_caps', [])
            volumes = coin_data.get('total_volumes', [])
            
            if not prices:
                logger.warning("No price data available")
                return None
            
            # Create DataFrame
            df_data = []
            for i, price_point in enumerate(prices):
                timestamp = price_point[0]
                price = price_point[1]
                
                # Get corresponding market cap and volume
                market_cap = market_caps[i][1] if i < len(market_caps) else 0
                volume = volumes[i][1] if i < len(volumes) else 0
                
                df_data.append({
                    'Date': pd.to_datetime(timestamp, unit='ms'),
                    'Open': price,  # CoinGecko returns single daily price
                    'High': price,
                    'Low': price,
                    'Close': price,
                    'Volume': volume,
                    'MarketCap': market_cap
                })
            
            df = pd.DataFrame(df_data)
            df.set_index('Date', inplace=True)
            df.sort_index(inplace=True)
            
            logger.debug(f"Formatted {len(df)} data points for {coin_info.get('id', 'unknown')}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to format market data: {e}")
            return None
    
    def get_exchanges_list(self) -> List[Dict]:
        """Get list of available exchanges"""
        def _make_request():
            url = f"{self.base_url}/exchanges"
            response = self.session.get(url, timeout=self.timeout)
            
            if not self.validate_response(response):
                return None
                
            return response.json()
        
        start_time = time.time()
        result = self.retry_request(_make_request)
        response_time = time.time() - start_time
        
        if result:
            logger.info(f"Retrieved {len(result)} exchanges from CoinGecko")
            self.log_api_usage("exchanges", "success", response_time)
            return result
        else:
            logger.error("Failed to retrieve exchanges list")
            self.log_api_usage("exchanges", "failure", response_time)
            return []
    
    def validate_coin_id(self, coin_id: str) -> bool:
        """Validate if a coin ID exists in CoinGecko"""
        def _make_request():
            url = f"{self.base_url}/coins/{coin_id}"
            params = {'localization': 'false', 'tickers': 'false', 'market_data': 'false'}
            response = self.session.get(url, params=params, timeout=self.timeout)
            return self.validate_response(response)
        
        result = self.retry_request(_make_request)
        return result is not None
    
    def get_api_status(self) -> Dict:
        """Get CoinGecko API status and rate limit information"""
        def _make_request():
            url = f"{self.base_url}/ping"
            response = self.session.get(url, timeout=self.timeout)
            
            if not self.validate_response(response):
                return {'status': 'error'}
                
            data = response.json()
            # Add rate limit headers if available
            if hasattr(response, 'headers'):
                data['rate_limit_remaining'] = response.headers.get('x-ratelimit-remaining')
                data['rate_limit_reset'] = response.headers.get('x-ratelimit-reset')
            
            return data
        
        result = self.retry_request(_make_request)
        return result or {'status': 'error'}
    
    # Cache management methods
    
    def _load_cache(self) -> None:
        """Load cache from file"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    self.api_cache = json.load(f)
                logger.debug(f"Loaded {len(self.api_cache)} cache entries from {self.cache_file}")
            else:
                self.api_cache = {}
        except Exception as e:
            logger.warning(f"Failed to load cache file: {e}")
            self.api_cache = {}
    
    def _save_cache(self) -> None:
        """Save cache to file"""
        if not self.cache_enabled:
            return
            
        try:
            # Check cache size limit
            if self.max_cache_size_mb > 0:
                cache_str = json.dumps(self.api_cache)
                cache_size_mb = len(cache_str.encode('utf-8')) / (1024 * 1024)
                if cache_size_mb > self.max_cache_size_mb:
                    logger.warning(f"Cache size ({cache_size_mb:.1f}MB) exceeds limit ({self.max_cache_size_mb}MB), clearing oldest entries")
                    self._trim_cache()
            
            with open(self.cache_file, 'w') as f:
                json.dump(self.api_cache, f, indent=2)
            logger.debug(f"Saved {len(self.api_cache)} cache entries to {self.cache_file}")
        except Exception as e:
            logger.error(f"Failed to save cache file: {e}")
    
    def _get_cache_key(self, endpoint: str, params: Dict = None) -> str:
        """Generate cache key for endpoint and parameters"""
        if params:
            param_str = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
            return f"{endpoint}?{param_str}"
        return endpoint
    
    def _get_from_cache(self, cache_key: str, ttl_hours: int) -> Optional[Dict]:
        """Get data from cache if valid and not expired"""
        if not self.cache_enabled or cache_key not in self.api_cache:
            return None
        
        cache_entry = self.api_cache[cache_key]
        cached_time = datetime.fromisoformat(cache_entry['timestamp'])
        age_hours = (datetime.now() - cached_time).total_seconds() / 3600
        
        if age_hours > ttl_hours:
            logger.debug(f"Cache entry expired for {cache_key} (age: {age_hours:.1f}h)")
            del self.api_cache[cache_key]
            return None
        
        logger.debug(f"Cache hit for {cache_key} (age: {age_hours:.1f}h)")
        return cache_entry['data']
    
    def _store_in_cache(self, cache_key: str, data: Dict) -> None:
        """Store data in cache with timestamp"""
        if not self.cache_enabled:
            return
            
        self.api_cache[cache_key] = {
            'data': data,
            'timestamp': datetime.now().isoformat()
        }
        logger.debug(f"Cached data for {cache_key}")
    
    def _clear_expired_entries(self) -> None:
        """Remove expired entries from cache"""
        if not self.cache_enabled:
            return
            
        expired_keys = []
        now = datetime.now()
        
        for cache_key, cache_entry in self.api_cache.items():
            cached_time = datetime.fromisoformat(cache_entry['timestamp'])
            age_hours = (now - cached_time).total_seconds() / 3600
            
            # Use max TTL for general cleanup
            max_ttl = max(self.exchange_data_ttl_hours, self.market_data_ttl_hours, self.coin_details_ttl_hours)
            if age_hours > max_ttl:
                expired_keys.append(cache_key)
        
        for key in expired_keys:
            del self.api_cache[key]
        
        if expired_keys:
            logger.info(f"Cleared {len(expired_keys)} expired cache entries")
            self._save_cache()
    
    def _trim_cache(self) -> None:
        """Remove oldest cache entries to stay under size limit"""
        if not self.api_cache:
            return
            
        # Sort by timestamp (oldest first)
        sorted_items = sorted(self.api_cache.items(), 
                            key=lambda x: x[1]['timestamp'])
        
        # Remove oldest 25% of entries
        remove_count = len(sorted_items) // 4
        for i in range(remove_count):
            key = sorted_items[i][0]
            del self.api_cache[key]
        
        logger.info(f"Trimmed {remove_count} oldest cache entries")
    
    def clear_cache(self) -> None:
        """Clear all cache entries"""
        self.api_cache = {}
        if os.path.exists(self.cache_file):
            os.remove(self.cache_file)
        logger.info("Cache cleared")
