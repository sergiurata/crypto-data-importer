"""
Abstract Data Provider Base Class
Defines the interface for cryptocurrency data providers
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import time
import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)


class AbstractDataProvider(ABC):
    """Abstract base class for cryptocurrency data providers"""
    
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.last_request_time = 0
        self.rate_limit_delay = config.getfloat('IMPORT', 'rate_limit_delay')
        self.timeout = config.getint('API', 'timeout_seconds')
        self.retry_attempts = config.getint('API', 'retry_attempts')
    
    @abstractmethod
    def get_all_coins(self) -> List[Dict]:
        """Get list of all available coins
        
        Returns:
            List of dictionaries containing coin information
        """
        pass
    
    @abstractmethod
    def get_market_data(self, coin_id: str, days: int = 365) -> Optional[Dict]:
        """Get historical market data for a specific coin
        
        Args:
            coin_id: Unique identifier for the coin
            days: Number of days of historical data
            
        Returns:
            Dictionary containing market data or None if failed
        """
        pass
    
    @abstractmethod
    def get_exchange_data(self, coin_id: str) -> Optional[Dict]:
        """Get exchange-specific data for a coin
        
        Args:
            coin_id: Unique identifier for the coin
            
        Returns:
            Dictionary containing exchange data or None if failed
        """
        pass
    
    def handle_rate_limiting(self):
        """Handle rate limiting between API requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def retry_request(self, request_func, *args, **kwargs):
        """Retry a request with exponential backoff
        
        Args:
            request_func: Function to execute
            *args, **kwargs: Arguments to pass to the function
            
        Returns:
            Result of the function or None if all attempts failed
        """
        for attempt in range(self.retry_attempts):
            try:
                self.handle_rate_limiting()
                result = request_func(*args, **kwargs)
                return result
                
            except requests.exceptions.RequestException as e:
                if attempt < self.retry_attempts - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"All retry attempts failed: {e}")
                    return None
            except Exception as e:
                logger.error(f"Unexpected error in request: {e}")
                return None
        
        return None
    
    def validate_response(self, response: requests.Response) -> bool:
        """Validate API response
        
        Args:
            response: HTTP response object
            
        Returns:
            True if response is valid, False otherwise
        """
        try:
            response.raise_for_status()
            return True
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            return False
        except Exception as e:
            logger.error(f"Response validation error: {e}")
            return False
    
    def get_default_headers(self) -> Dict[str, str]:
        """Get default headers for API requests
        
        Returns:
            Dictionary of default headers
        """
        return {
            'User-Agent': 'Crypto-Data-Importer/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
    
    def log_api_usage(self, endpoint: str, status: str, response_time: float = None):
        """Log API usage for monitoring
        
        Args:
            endpoint: API endpoint called
            status: Status of the request (success/failure)
            response_time: Time taken for the request
        """
        log_msg = f"API Call - Endpoint: {endpoint}, Status: {status}"
        if response_time:
            log_msg += f", Response Time: {response_time:.2f}s"
        
        logger.debug(log_msg)
