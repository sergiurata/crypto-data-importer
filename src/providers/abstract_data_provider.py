"""
Abstract Data Provider Base Class
Defines the interface for cryptocurrency data providers
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Deque
import time
import logging
import requests
from datetime import datetime
from collections import deque

logger = logging.getLogger(__name__)


class AbstractDataProvider(ABC):
    """Abstract base class for cryptocurrency data providers"""
    
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.last_request_time = 0
        self.timeout = config.getint('API', 'timeout_seconds')
        self.retry_attempts = config.getint('API', 'retry_attempts')
        
        # Adaptive rate limiting initialization
        self.adaptive_rate_limiting = config.getboolean('ADAPTIVE_RATE_LIMITING', 'enable_adaptive_rate_limiting', True)
        
        if self.adaptive_rate_limiting:
            # Dynamic rate limiting settings - start with existing API/IMPORT settings
            self.current_requests_per_minute = config.getfloat('API', 'requests_per_minute', 24)
            self.current_rate_limit_delay = config.getfloat('IMPORT', 'rate_limit_delay', 2.5)
            
            # Rate limiting bounds
            self.min_requests_per_minute = config.getfloat('ADAPTIVE_RATE_LIMITING', 'min_requests_per_minute', 10)
            self.max_requests_per_minute = config.getfloat('ADAPTIVE_RATE_LIMITING', 'max_requests_per_minute', 50)
            self.min_rate_limit_delay = config.getfloat('ADAPTIVE_RATE_LIMITING', 'min_rate_limit_delay', 1.0)
            self.max_rate_limit_delay = config.getfloat('ADAPTIVE_RATE_LIMITING', 'max_rate_limit_delay', 10.0)
            
            # Adjustment parameters
            self.rate_limit_adjustment_factor = config.getfloat('ADAPTIVE_RATE_LIMITING', 'rate_limit_adjustment_factor', 0.8)
            self.rate_limit_increase_factor = config.getfloat('ADAPTIVE_RATE_LIMITING', 'rate_limit_increase_factor', 1.2)
            self.consecutive_successes_threshold = config.getint('ADAPTIVE_RATE_LIMITING', 'consecutive_successes_threshold', 10)
            self.consecutive_failures_threshold = config.getint('ADAPTIVE_RATE_LIMITING', 'consecutive_failures_threshold', 3)
            
            # Monitoring window
            self.monitoring_window_size = config.getint('ADAPTIVE_RATE_LIMITING', 'monitoring_window_size', 20)
            
            # Request history tracking
            self.request_history: Deque[Dict] = deque(maxlen=self.monitoring_window_size)
            self.consecutive_successes = 0
            self.consecutive_failures = 0
            
            self.rate_limit_delay = self.current_rate_limit_delay
            logger.info(f"Adaptive rate limiting enabled: {self.current_requests_per_minute} req/min, {self.current_rate_limit_delay}s delay")
        else:
            # Use static rate limiting from original config
            self.rate_limit_delay = config.getfloat('IMPORT', 'rate_limit_delay', 2.5)
            logger.info(f"Static rate limiting: {self.rate_limit_delay}s delay")
    
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
    
    def retry_request(self, request_func, endpoint: str = "unknown", *args, **kwargs):
        """Retry a request with exponential backoff and adaptive rate limiting
        
        Args:
            request_func: Function to execute
            endpoint: API endpoint for tracking (used for adaptive rate limiting)
            *args, **kwargs: Arguments to pass to the function
            
        Returns:
            Result of the function or None if all attempts failed
        """
        start_time = time.time()
        last_response = None
        
        for attempt in range(self.retry_attempts):
            try:
                self.handle_rate_limiting()
                response = request_func(*args, **kwargs)
                last_response = response
                
                if response and hasattr(response, 'status_code'):
                    # Record successful request for adaptive rate limiting
                    response_time = time.time() - start_time
                    self.record_request_result(endpoint, True, response_time, response.status_code)
                    return response
                else:
                    # Handle case where response is None or doesn't have status_code
                    response_time = time.time() - start_time
                    self.record_request_result(endpoint, response is not None, response_time)
                    return response
                
            except requests.exceptions.HTTPError as e:
                response_time = time.time() - start_time
                status_code = e.response.status_code if e.response else None
                
                # Record failed request for adaptive rate limiting
                self.record_request_result(endpoint, False, response_time, status_code)
                
                # Handle rate limiting errors specially
                if status_code in [429, 503]:
                    if attempt < self.retry_attempts - 1:
                        wait_time = min(2 ** attempt * 5, 60)  # Longer wait for rate limiting
                        logger.warning(f"Rate limit hit (attempt {attempt + 1}): {status_code}")
                        logger.info(f"Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
                        continue
                
                # Handle other HTTP errors
                if attempt < self.retry_attempts - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"HTTP error (attempt {attempt + 1}): {e}")
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"All retry attempts failed with HTTP error: {e}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                response_time = time.time() - start_time
                self.record_request_result(endpoint, False, response_time)
                
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
                response_time = time.time() - start_time
                self.record_request_result(endpoint, False, response_time)
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
    
    # Adaptive Rate Limiting Methods
    
    def record_request_result(self, endpoint: str, success: bool, response_time: float = None, status_code: int = None):
        """Record the result of an API request for adaptive rate limiting"""
        if not self.adaptive_rate_limiting:
            return
            
        request_record = {
            'timestamp': time.time(),
            'endpoint': endpoint,
            'success': success,
            'response_time': response_time,
            'status_code': status_code
        }
        
        self.request_history.append(request_record)
        
        # Update consecutive counters
        if success:
            self.consecutive_successes += 1
            self.consecutive_failures = 0
        else:
            self.consecutive_failures += 1
            self.consecutive_successes = 0
        
        # Trigger rate adjustment based on patterns
        self._evaluate_rate_adjustment()
    
    def _evaluate_rate_adjustment(self):
        """Evaluate whether to adjust rate limiting based on recent performance"""
        if not self.adaptive_rate_limiting or len(self.request_history) < 5:
            return
        
        # Check for consecutive failures (immediate slowdown)
        if self.consecutive_failures >= self.consecutive_failures_threshold:
            self._decrease_rate_limit("consecutive failures detected")
            self.consecutive_failures = 0  # Reset counter after adjustment
        
        # Check for sustained success (gradual speedup)
        elif self.consecutive_successes >= self.consecutive_successes_threshold:
            self._increase_rate_limit("sustained success pattern")
            self.consecutive_successes = 0  # Reset counter after adjustment
        
        # Analyze recent window for rate limiting error patterns
        elif len(self.request_history) >= self.monitoring_window_size:
            self._analyze_recent_performance()
    
    def _analyze_recent_performance(self):
        """Analyze recent request performance for rate adjustment"""
        recent_requests = list(self.request_history)
        
        # Count failures in recent window
        failures = sum(1 for r in recent_requests if not r['success'])
        failure_rate = failures / len(recent_requests)
        
        # Count specific rate limiting errors (429, 503)
        rate_limit_errors = sum(1 for r in recent_requests 
                               if r.get('status_code') in [429, 503])
        rate_limit_error_rate = rate_limit_errors / len(recent_requests)
        
        # Count timeouts
        timeout_errors = sum(1 for r in recent_requests 
                           if r.get('response_time', 0) > self.timeout * 0.8)
        timeout_rate = timeout_errors / len(recent_requests)
        
        # Adjust based on error patterns
        if rate_limit_error_rate > 0.2:  # More than 20% rate limit errors
            self._decrease_rate_limit(f"high rate limit error rate: {rate_limit_error_rate:.1%}")
        elif timeout_rate > 0.3:  # More than 30% timeouts
            self._decrease_rate_limit(f"high timeout rate: {timeout_rate:.1%}")
        elif failure_rate < 0.05 and rate_limit_error_rate == 0:  # Less than 5% failures, no rate limiting
            self._increase_rate_limit(f"excellent performance: {failure_rate:.1%} failure rate")
    
    def _decrease_rate_limit(self, reason: str):
        """Decrease request rate (increase delay) due to errors"""
        old_delay = self.current_rate_limit_delay
        old_rpm = self.current_requests_per_minute
        
        # Increase delay
        self.current_rate_limit_delay = min(
            self.current_rate_limit_delay / self.rate_limit_adjustment_factor,
            self.max_rate_limit_delay
        )
        
        # Decrease requests per minute
        self.current_requests_per_minute = max(
            self.current_requests_per_minute * self.rate_limit_adjustment_factor,
            self.min_requests_per_minute
        )
        
        # Update the active delay
        self.rate_limit_delay = self.current_rate_limit_delay
        
        if old_delay != self.current_rate_limit_delay:
            logger.warning(f"Rate limit decreased due to {reason}: "
                         f"{old_rpm:.1f}→{self.current_requests_per_minute:.1f} req/min, "
                         f"{old_delay:.1f}→{self.current_rate_limit_delay:.1f}s delay")
    
    def _increase_rate_limit(self, reason: str):
        """Increase request rate (decrease delay) due to good performance"""
        old_delay = self.current_rate_limit_delay
        old_rpm = self.current_requests_per_minute
        
        # Decrease delay
        self.current_rate_limit_delay = max(
            self.current_rate_limit_delay * self.rate_limit_adjustment_factor,
            self.min_rate_limit_delay
        )
        
        # Increase requests per minute
        self.current_requests_per_minute = min(
            self.current_requests_per_minute * self.rate_limit_increase_factor,
            self.max_requests_per_minute
        )
        
        # Update the active delay
        self.rate_limit_delay = self.current_rate_limit_delay
        
        if old_delay != self.current_rate_limit_delay:
            logger.info(f"Rate limit increased due to {reason}: "
                       f"{old_rpm:.1f}→{self.current_requests_per_minute:.1f} req/min, "
                       f"{old_delay:.1f}→{self.current_rate_limit_delay:.1f}s delay")
    
    def get_current_rate_stats(self) -> Dict:
        """Get current rate limiting statistics"""
        if not self.adaptive_rate_limiting:
            return {"mode": "static", "delay": self.rate_limit_delay}
        
        recent_failures = sum(1 for r in self.request_history if not r['success'])
        recent_successes = len(self.request_history) - recent_failures
        
        return {
            "mode": "adaptive",
            "current_requests_per_minute": self.current_requests_per_minute,
            "current_delay": self.current_rate_limit_delay,
            "consecutive_successes": self.consecutive_successes,
            "consecutive_failures": self.consecutive_failures,
            "recent_success_rate": recent_successes / max(len(self.request_history), 1),
            "monitoring_window_size": len(self.request_history)
        }
