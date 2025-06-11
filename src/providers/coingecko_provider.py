"""
CoinGecko Data Provider Implementation
Handles all CoinGecko API interactions
"""

import pandas as pd
import time
from typing import Dict, List, Optional
from datetime import datetime
import logging

from abstract_data_provider import AbstractDataProvider

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
        def _make_request():
            url = f"{self.base_url}/coins/{coin_id}/market_chart"
            params = {
                'vs_currency': 'usd',
                'days': days,
                'interval': 'daily'
            }
            
            response = self.session.get(url, params=params, timeout=self.timeout)
            
            if not self.validate_response(response):
                return None
                
            return response.json()
        
        start_time = time.time()
        result = self.retry_request(_make_request)
        response_time = time.time() - start_time
        
        endpoint = f"coins/{coin_id}/market_chart"
        if result:
            self.log_api_usage(endpoint, "success", response_time)
            return result
        else:
            logger.error(f"Failed to get market data for {coin_id}")
            self.log_api_usage(endpoint, "failure", response_time)
            return None
    
    def get_exchange_data(self, coin_id: str) -> Optional[Dict]:
        """Get exchange-specific data for a coin"""
        def _make_request():
            url = f"{self.base_url}/coins/{coin_id}"
            params = {
                'tickers': 'true',
                'community_data': 'false',
                'developer_data': 'false',
                'sparkline': 'false'
            }
            
            response = self.session.get(url, params=params, timeout=self.timeout)
            
            if not self.validate_response(response):
                return None
                
            return response.json()
        
        start_time = time.time()
        result = self.retry_request(_make_request)
        response_time = time.time() - start_time
        
        endpoint = f"coins/{coin_id}"
        if result:
            self.log_api_usage(endpoint, "success", response_time)
            return result
        else:
            logger.error(f"Failed to get exchange data for {coin_id}")
            self.log_api_usage(endpoint, "failure", response_time)
            return None
    
    def get_coin_details(self, coin_id: str) -> Optional[Dict]:
        """Get detailed information about a specific coin"""
        def _make_request():
            url = f"{self.base_url}/coins/{coin_id}"
            params = {
                'localization': 'false',
                'tickers': 'false',
                'market_data': 'true',
                'community_data': 'false',
                'developer_data': 'false',
                'sparkline': 'false'
            }
            
            response = self.session.get(url, params=params, timeout=self.timeout)
            
            if not self.validate_response(response):
                return None
                
            return response.json()
        
        start_time = time.time()
        result = self.retry_request(_make_request)
        response_time = time.time() - start_time
        
        endpoint = f"coins/{coin_id}/details"
        if result:
            self.log_api_usage(endpoint, "success", response_time)
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
