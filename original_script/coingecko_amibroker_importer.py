import requests
import pandas as pd
import time
import win32com.client
import logging
import os
import configparser
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ConfigManager:
    """Manages configuration files for the CoinGecko AmiBroker Importer"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "config.ini"
        self.config = configparser.ConfigParser()
        self.default_config = self._get_default_config()
        self.load_config()
    
    def _get_default_config(self) -> Dict:
        """Return default configuration values"""
        return {
            'DATABASE': {
                'database_path': r'C:\AmiBroker\Databases\Crypto\crypto.adb',
                'create_if_not_exists': 'true',
                'auto_backup': 'false',
                'backup_path': r'C:\AmiBroker\Backups\Crypto'
            },
            'IMPORT': {
                'max_coins': '500',
                'min_market_cap': '10000000',
                'historical_days': '365',
                'force_full_update': 'false',
                'rate_limit_delay': '1.5'
            },
            'MAPPING': {
                'use_cached_mapping': 'true',
                'mapping_file': 'coingecko_kraken_mapping.json',
                'rebuild_mapping_days': '7',
                'cache_expiry_hours': '24'
            },
            'FILTERING': {
                'include_kraken_only': 'false',
                'exclude_stablecoins': 'false',
                'min_volume_24h': '0',
                'excluded_symbols': '',
                'included_symbols': ''
            },
            'UPDATES': {
                'auto_update_enabled': 'true',
                'update_frequency_hours': '6',
                'update_days_back': '7',
                'update_on_startup': 'true'
            },
            'LOGGING': {
                'log_level': 'INFO',
                'log_file': 'crypto_importer.log',
                'max_log_size_mb': '10',
                'backup_count': '5'
            },
            'API': {
                'coingecko_api_key': '',
                'requests_per_minute': '40',
                'timeout_seconds': '30',
                'retry_attempts': '3'
            }
        }
    
    def create_default_config(self):
        """Create a default configuration file"""
        logger.info(f"Creating default configuration file: {self.config_path}")
        
        for section, options in self.default_config.items():
            self.config.add_section(section)
            for key, value in options.items():
                self.config.set(section, key, value)
        
        # Add comments to the config file
        config_content = self._generate_config_with_comments()
        
        with open(self.config_path, 'w') as config_file:
            config_file.write(config_content)
        
        logger.info(f"Default configuration created at: {self.config_path}")
    
    def _generate_config_with_comments(self) -> str:
        """Generate configuration file with detailed comments"""
        return '''# CoinGecko AmiBroker Importer Configuration
# ==========================================

[DATABASE]
# Path to AmiBroker database file
database_path = C:\\AmiBroker\\Databases\\Crypto\\crypto.adb

# Create database if it doesn't exist
create_if_not_exists = true

# Enable automatic database backup before major operations
auto_backup = false

# Path for database backups
backup_path = C:\\AmiBroker\\Backups\\Crypto

[IMPORT]
# Maximum number of coins to import (use 0 for unlimited)
max_coins = 500

# Minimum market cap filter (in USD)
min_market_cap = 10000000

# Number of days of historical data to import
historical_days = 365

# Force complete data refresh on every run
force_full_update = false

# Delay between API calls (seconds) to respect rate limits
rate_limit_delay = 1.5

[MAPPING]
# Use cached Kraken mapping to speed up startup
use_cached_mapping = true

# File to store CoinGecko-Kraken mapping
mapping_file = coingecko_kraken_mapping.json

# Rebuild mapping if cache is older than this (days)
rebuild_mapping_days = 7

# Cache expiry time for mapping data (hours)
cache_expiry_hours = 24

[FILTERING]
# Import only coins available on Kraken
include_kraken_only = false

# Exclude stablecoins from import
exclude_stablecoins = false

# Minimum 24h trading volume filter (in USD)
min_volume_24h = 0

# Comma-separated list of symbols to exclude (e.g., USDT,USDC,DAI)
excluded_symbols = 

# Comma-separated list of symbols to include (empty = include all)
included_symbols = 

[UPDATES]
# Enable automatic updates of existing data
auto_update_enabled = true

# Frequency of automatic updates (hours)
update_frequency_hours = 6

# Number of days to look back for updates
update_days_back = 7

# Run update check on script startup
update_on_startup = true

[LOGGING]
# Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
log_level = INFO

# Log file path (empty = console only)
log_file = crypto_importer.log

# Maximum log file size (MB)
max_log_size_mb = 10

# Number of backup log files to keep
backup_count = 5

[API]
# CoinGecko API key (optional, for higher rate limits)
coingecko_api_key = 

# Maximum requests per minute
requests_per_minute = 40

# Request timeout in seconds
timeout_seconds = 30

# Number of retry attempts for failed requests
retry_attempts = 3
'''
    
    def load_config(self):
        """Load configuration from file, create default if not exists"""
        if not os.path.exists(self.config_path):
            logger.info(f"Configuration file not found: {self.config_path}")
            self.create_default_config()
        
        try:
            self.config.read(self.config_path)
            logger.info(f"Configuration loaded from: {self.config_path}")
            self._validate_config()
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            logger.info("Using default configuration")
            self._load_defaults()
    
    def _load_defaults(self):
        """Load default configuration values"""
        for section, options in self.default_config.items():
            self.config.add_section(section)
            for key, value in options.items():
                self.config.set(section, key, value)
    
    def _validate_config(self):
        """Validate configuration values"""
        # Check required sections exist
        required_sections = ['DATABASE', 'IMPORT', 'MAPPING']
        for section in required_sections:
            if not self.config.has_section(section):
                logger.warning(f"Missing configuration section: {section}")
                self.config.add_section(section)
        
        # Add missing options with defaults
        for section, options in self.default_config.items():
            if not self.config.has_section(section):
                self.config.add_section(section)
            
            for key, default_value in options.items():
                if not self.config.has_option(section, key):
                    logger.info(f"Adding missing config option: [{section}] {key}")
                    self.config.set(section, key, default_value)
    
    def get(self, section: str, key: str, fallback: str = '') -> str:
        """Get configuration value as string"""
        return self.config.get(section, key, fallback=fallback)
    
    def getint(self, section: str, key: str, fallback: int = 0) -> int:
        """Get configuration value as integer"""
        return self.config.getint(section, key, fallback=fallback)
    
    def getfloat(self, section: str, key: str, fallback: float = 0.0) -> float:
        """Get configuration value as float"""
        return self.config.getfloat(section, key, fallback=fallback)
    
    def getboolean(self, section: str, key: str, fallback: bool = False) -> bool:
        """Get configuration value as boolean"""
        return self.config.getboolean(section, key, fallback=fallback)
    
    def getlist(self, section: str, key: str, delimiter: str = ',') -> List[str]:
        """Get configuration value as list"""
        value = self.get(section, key)
        if not value.strip():
            return []
        return [item.strip() for item in value.split(delimiter) if item.strip()]
    
    def set_value(self, section: str, key: str, value: str):
        """Set configuration value"""
        if not self.config.has_section(section):
            self.config.add_section(section)
        self.config.set(section, key, str(value))
    
    def save_config(self):
        """Save current configuration to file"""
        try:
            with open(self.config_path, 'w') as config_file:
                self.config.write(config_file)
            logger.info(f"Configuration saved to: {self.config_path}")
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
    
    def print_config(self):
        """Print current configuration"""
        logger.info("Current Configuration:")
        for section in self.config.sections():
            logger.info(f"  [{section}]")
            for key, value in self.config.items(section):
                logger.info(f"    {key} = {value}")

class CoinGeckoAmiBrokerImporter:
    def __init__(self, config_path: Optional[str] = None, database_path: Optional[str] = None):
        # Load configuration
        self.config = ConfigManager(config_path)
        
        # Setup logging based on config
        self._setup_logging()
        
        # Override database path if provided
        if database_path:
            self.database_path = database_path
        else:
            self.database_path = self.config.get('DATABASE', 'database_path')
        
        # API configuration
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session = requests.Session()
        
        # Set API key if provided
        api_key = self.config.get('API', 'coingecko_api_key')
        if api_key:
            self.session.headers.update({'x-cg-demo-api-key': api_key})
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Rate limiting configuration
        self.rate_limit_delay = self.config.getfloat('IMPORT', 'rate_limit_delay')
        self.requests_per_minute = self.config.getint('API', 'requests_per_minute')
        self.timeout = self.config.getint('API', 'timeout_seconds')
        self.retry_attempts = self.config.getint('API', 'retry_attempts')
        
        # Initialize AmiBroker COM object
        try:
            self.ab = win32com.client.Dispatch("Broker.Application")
            logger.info("AmiBroker COM connection established")
            
            # Load specific database if provided
            if self.database_path:
                self._load_database()
            else:
                current_db = self._get_current_database()
                logger.info(f"Using current AmiBroker database: {current_db}")
                
        except Exception as e:
            logger.error(f"Failed to connect to AmiBroker: {e}")
            raise
        
        # Cache for Kraken data
        self.kraken_pairs = {}  # Maps Kraken symbol to asset info
        self.coingecko_kraken_map = {}  # Maps CoinGecko ID to Kraken symbol
        
        # Load Kraken data if needed
        if not self.config.getboolean('FILTERING', 'include_kraken_only'):
            logger.info("Kraken filtering disabled, loading Kraken data for mapping only")
        self._load_kraken_data()
    
    def _setup_logging(self):
        """Setup logging based on configuration"""
        log_level = self.config.get('LOGGING', 'log_level', 'INFO')
        log_file = self.config.get('LOGGING', 'log_file')
        
        # Convert string log level to logging constant
        numeric_level = getattr(logging, log_level.upper(), logging.INFO)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(numeric_level)
        
        # Clear existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Setup console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(numeric_level)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        
        # Setup file handler if specified
        if log_file:
            try:
                from logging.handlers import RotatingFileHandler
                max_size = self.config.getint('LOGGING', 'max_log_size_mb') * 1024 * 1024
                backup_count = self.config.getint('LOGGING', 'backup_count')
                
                file_handler = RotatingFileHandler(
                    log_file, maxBytes=max_size, backupCount=backup_count
                )
                file_handler.setLevel(numeric_level)
                file_handler.setFormatter(formatter)
                root_logger.addHandler(file_handler)
                
                logger.info(f"Logging to file: {log_file}")
            except Exception as e:
                logger.warning(f"Could not setup file logging: {e}")
    
    def _should_rebuild_mapping(self) -> bool:
        """Check if mapping should be rebuilt based on configuration"""
        mapping_file = self.config.get('MAPPING', 'mapping_file')
        
        if not os.path.exists(mapping_file):
            return True
        
        # Check file age
        rebuild_days = self.config.getint('MAPPING', 'rebuild_mapping_days')
        if rebuild_days <= 0:
            return False
        
        file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(mapping_file))
        return file_age.days >= rebuild_days
    
    def _load_database(self):
        """Load specified AmiBroker database"""
        try:
            if not self.database_path:
                logger.warning("No database path specified")
                return
            
            # Check if database path exists
            if not os.path.exists(self.database_path):
                logger.error(f"Database path does not exist: {self.database_path}")
                raise FileNotFoundError(f"Database not found: {self.database_path}")
            
            # Get current database for comparison
            current_db = self._get_current_database()
            
            # Normalize paths for comparison
            current_db_normalized = os.path.normpath(current_db).lower() if current_db else ""
            target_db_normalized = os.path.normpath(self.database_path).lower()
            
            if current_db_normalized == target_db_normalized:
                logger.info(f"Database already loaded: {self.database_path}")
                return
            
            logger.info(f"Loading AmiBroker database: {self.database_path}")
            
            # Load the database
            result = self.ab.LoadDatabase(self.database_path)
            
            if result:
                logger.info(f"Successfully loaded database: {self.database_path}")
                
                # Verify the database was loaded
                new_current_db = self._get_current_database()
                if new_current_db:
                    logger.info(f"Active database: {new_current_db}")
                else:
                    logger.warning("Could not verify database loading")
            else:
                logger.error(f"Failed to load database: {self.database_path}")
                raise Exception(f"AmiBroker failed to load database: {self.database_path}")
                
        except Exception as e:
            logger.error(f"Error loading database: {e}")
            raise
    
    def _get_current_database(self) -> Optional[str]:
        """Get the path of currently loaded AmiBroker database"""
        try:
            # Try to get the database path through various methods
            
            # Method 1: Try DatabasePath property
            try:
                db_path = self.ab.DatabasePath
                if db_path:
                    return db_path
            except AttributeError:
                pass
            
            # Method 2: Try through Documents collection
            try:
                if hasattr(self.ab, 'Documents') and self.ab.Documents.Count > 0:
                    doc = self.ab.Documents(0)
                    if hasattr(doc, 'Path'):
                        return doc.Path
            except (AttributeError, IndexError):
                pass
            
            # Method 3: Try ActiveDocument
            try:
                if hasattr(self.ab, 'ActiveDocument') and self.ab.ActiveDocument:
                    active_doc = self.ab.ActiveDocument
                    if hasattr(active_doc, 'Path'):
                        return active_doc.Path
            except AttributeError:
                pass
            
            logger.debug("Could not determine current database path")
            return None
            
        except Exception as e:
            logger.debug(f"Error getting current database: {e}")
            return None
    
    def create_database(self, db_path: str, overwrite: bool = False) -> bool:
        """Create a new AmiBroker database"""
        try:
            # Check if database already exists
            if os.path.exists(db_path):
                if not overwrite:
                    logger.error(f"Database already exists: {db_path}")
                    return False
                else:
                    logger.info(f"Overwriting existing database: {db_path}")
            
            # Ensure directory exists
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
                logger.info(f"Created directory: {db_dir}")
            
            # Create new database
            logger.info(f"Creating new AmiBroker database: {db_path}")
            result = self.ab.NewDatabase(db_path)
            
            if result:
                logger.info(f"Successfully created database: {db_path}")
                self.database_path = db_path
                return True
            else:
                logger.error(f"Failed to create database: {db_path}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating database: {e}")
            return False
    
    def list_recent_databases(self, max_count: int = 10) -> List[str]:
        """List recently used AmiBroker databases"""
        try:
            recent_dbs = []
            
            # Try to get recent files from AmiBroker
            try:
                if hasattr(self.ab, 'RecentFiles'):
                    recent_files = self.ab.RecentFiles
                    for i in range(min(recent_files.Count, max_count)):
                        recent_dbs.append(recent_files(i))
            except (AttributeError, Exception):
                pass
            
            # Alternative: Check Windows registry for recent files
            if not recent_dbs:
                try:
                    import winreg
                    key_path = r"SOFTWARE\AmiBroker\Recent File List"
                    with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path) as key:
                        for i in range(max_count):
                            try:
                                value_name = f"File{i+1}"
                                db_path, _ = winreg.QueryValueEx(key, value_name)
                                if os.path.exists(db_path):
                                    recent_dbs.append(db_path)
                            except FileNotFoundError:
                                break
                except Exception as e:
                    logger.debug(f"Could not read recent databases from registry: {e}")
            
            return recent_dbs
            
        except Exception as e:
            logger.error(f"Error listing recent databases: {e}")
            return []
    def _load_kraken_data(self):
        """Load Kraken assets and create mapping with CoinGecko"""
        try:
            # Get Kraken assets
            response = self.session.get("https://api.kraken.com/0/public/Assets")
            if response.status_code == 200:
                data = response.json()
                if data.get('error') == []:
                    assets = data.get('result', {})
                    for symbol, asset_info in assets.items():
                        self.kraken_pairs[symbol] = asset_info
                    logger.info(f"Loaded {len(self.kraken_pairs)} Kraken assets")
                else:
                    logger.warning(f"Kraken API error: {data.get('error')}")
                    
            # Build CoinGecko to Kraken mapping using exchange data
            self._build_coingecko_kraken_mapping()
            
        except Exception as e:
            logger.error(f"Failed to load Kraken data: {e}")
    
    def _build_coingecko_kraken_mapping(self):
        """Build mapping between CoinGecko IDs and Kraken symbols using exchange data"""
        try:
            # Get exchange list from CoinGecko
            exchanges_url = f"{self.base_url}/exchanges"
            response = self.session.get(exchanges_url)
            
            if response.status_code != 200:
                logger.warning("Failed to get exchanges list from CoinGecko")
                return
                
            exchanges = response.json()
            kraken_exchange_id = None
            
            # Find Kraken exchange ID
            for exchange in exchanges:
                if exchange.get('id', '').lower() == 'kraken':
                    kraken_exchange_id = exchange['id']
                    break
            
            if not kraken_exchange_id:
                logger.warning("Kraken exchange not found in CoinGecko exchanges")
                return
            
            logger.info(f"Found Kraken exchange ID: {kraken_exchange_id}")
            
            # Also get Kraken asset pairs for additional metadata
            kraken_pairs_info = self._get_kraken_asset_pairs()
            
            # Get coins with exchange data
            coins_url = f"{self.base_url}/coins/list"
            params = {'include_platform': 'false'}
            response = self.session.get(coins_url, params=params)
            
            if response.status_code != 200:
                logger.warning("Failed to get coins list from CoinGecko")
                return
                
            all_coins = response.json()
            
            # Process coins in batches to get exchange data
            batch_size = 50
            mapped_count = 0
            
            for i in range(0, len(all_coins), batch_size):
                batch = all_coins[i:i + batch_size]
                batch_ids = [coin['id'] for coin in batch]
                
                # Get detailed coin data including tickers
                for coin_id in batch_ids:
                    try:
                        coin_detail_url = f"{self.base_url}/coins/{coin_id}"
                        params = {'tickers': 'true', 'community_data': 'false', 
                                'developer_data': 'false', 'sparkline': 'false'}
                        
                        detail_response = self.session.get(coin_detail_url, params=params)
                        
                        if detail_response.status_code == 200:
                            coin_data = detail_response.json()
                            
                            # Check tickers for Kraken
                            tickers = coin_data.get('tickers', [])
                            for ticker in tickers:
                                market = ticker.get('market', {})
                                if market.get('identifier', '').lower() == 'kraken':
                                    # Extract Kraken symbol from ticker
                                    base = ticker.get('base', '')
                                    target = ticker.get('target', '')
                                    
                                    # Map CoinGecko ID to Kraken symbol
                                    if base and target:
                                        kraken_symbol = f"{base}{target}"
                                        
                                        # Try to find the pair name from Kraken API
                                        pair_name = self._find_kraken_pair_name(base, target, kraken_pairs_info)
                                        
                                        self.coingecko_kraken_map[coin_id] = {
                                            'kraken_symbol': kraken_symbol,
                                            'base': base,
                                            'target': target,
                                            'ticker_url': ticker.get('trade_url', ''),
                                            'pair_name': pair_name or kraken_symbol,
                                            'alt_name': kraken_pairs_info.get(pair_name, {}).get('altname', '') if pair_name else ''
                                        }
                                        mapped_count += 1
                                        break
                        
                        # Rate limiting
                        time.sleep(0.1)
                        
                    except Exception as e:
                        logger.debug(f"Failed to get exchange data for {coin_id}: {e}")
                        continue
                
                logger.info(f"Processed {min(i + batch_size, len(all_coins))}/{len(all_coins)} coins")
                time.sleep(1)  # Rate limiting between batches
            
            logger.info(f"Built CoinGecko-Kraken mapping for {mapped_count} coins")
            
        except Exception as e:
            logger.error(f"Failed to build CoinGecko-Kraken mapping: {e}")
    
    def _get_kraken_asset_pairs(self):
        """Get Kraken asset pairs with their official names"""
        try:
            response = self.session.get("https://api.kraken.com/0/public/AssetPairs")
            if response.status_code == 200:
                data = response.json()
                if data.get('error') == []:
                    pairs = data.get('result', {})
                    logger.info(f"Retrieved {len(pairs)} Kraken asset pairs")
                    return pairs
                else:
                    logger.warning(f"Kraken AssetPairs API error: {data.get('error')}")
            return {}
        except Exception as e:
            logger.error(f"Failed to get Kraken asset pairs: {e}")
            return {}
    
    def _find_kraken_pair_name(self, base: str, target: str, kraken_pairs: dict) -> Optional[str]:
        """Find the official Kraken pair name for base/target currencies"""
        if not kraken_pairs:
            return None
        
        # Try different combinations to match Kraken's naming
        possible_combinations = [
            f"{base}{target}",
            f"{base.upper()}{target.upper()}",
            f"X{base}Z{target}",  # Kraken's extended format
            f"X{base.upper()}Z{target.upper()}",
            f"{base}{target}.d",  # Some pairs have .d suffix
        ]
        
        # Also try reversed combinations for some cases
        if target.upper() in ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'CHF', 'AUD']:
            possible_combinations.extend([
                f"Z{target}X{base}",
                f"Z{target.upper()}X{base.upper()}",
            ])
        
        for combo in possible_combinations:
            if combo in kraken_pairs:
                return combo
        
        # Try matching by altname
        for pair_name, pair_info in kraken_pairs.items():
            altname = pair_info.get('altname', '')
            if altname == f"{base}{target}" or altname == f"{base.upper()}{target.upper()}":
                return pair_name
        
        return None
    
    def get_all_coins(self) -> List[Dict]:
        """Get list of all coins from CoinGecko"""
        try:
            url = f"{self.base_url}/coins/list"
            response = self.session.get(url)
            response.raise_for_status()
            
            coins = response.json()
            logger.info(f"Retrieved {len(coins)} coins from CoinGecko")
            return coins
        except Exception as e:
            logger.error(f"Failed to get coins list: {e}")
            return []
    
    def get_coin_market_data(self, coin_id: str, days: Optional[int] = None) -> Optional[Dict]:
        """Get historical market data for a specific coin"""
        if days is None:
            days = self.config.getint('IMPORT', 'historical_days')
        
        try:
            url = f"{self.base_url}/coins/{coin_id}/market_chart"
            params = {
                'vs_currency': 'usd',
                'days': days,
                'interval': 'daily'
            }
            
            for attempt in range(self.retry_attempts):
                try:
                    response = self.session.get(url, params=params, timeout=self.timeout)
                    response.raise_for_status()
                    
                    data = response.json()
                    return data
                except requests.exceptions.RequestException as e:
                    if attempt < self.retry_attempts - 1:
                        logger.warning(f"Request failed for {coin_id}, attempt {attempt + 1}: {e}")
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    else:
                        raise
        except Exception as e:
            logger.error(f"Failed to get market data for {coin_id}: {e}")
            return None
    
    def _apply_filters(self, coin: Dict) -> bool:
        """Apply configured filters to determine if coin should be imported"""
        coin_id = coin['id']
        symbol = coin['symbol'].upper()
        
        # Check excluded symbols
        excluded_symbols = self.config.getlist('FILTERING', 'excluded_symbols')
        if symbol in excluded_symbols:
            logger.debug(f"Excluded symbol: {symbol}")
            return False
        
        # Check included symbols (if specified, only import these)
        included_symbols = self.config.getlist('FILTERING', 'included_symbols')
        if included_symbols and symbol not in included_symbols:
            logger.debug(f"Not in included symbols: {symbol}")
            return False
        
        # Check if Kraken-only mode is enabled
        if self.config.getboolean('FILTERING', 'include_kraken_only'):
            is_kraken, _, _ = self.is_kraken_tradeable(coin_id, symbol)
            if not is_kraken:
                logger.debug(f"Not on Kraken: {symbol}")
                return False
        
        # Check stablecoin exclusion
        if self.config.getboolean('FILTERING', 'exclude_stablecoins'):
            stablecoin_indicators = ['usd', 'usdt', 'usdc', 'dai', 'busd', 'tusd', 'usdn', 'fei']
            if any(indicator in symbol.lower() for indicator in stablecoin_indicators):
                logger.debug(f"Excluded stablecoin: {symbol}")
                return False
        
        return True
    
    def format_market_data(self, coin_data: Dict, coin_info: Dict) -> Optional[pd.DataFrame]:
        """Format CoinGecko data into AmiBroker-compatible format"""
        try:
            prices = coin_data.get('prices', [])
            market_caps = coin_data.get('market_caps', [])
            volumes = coin_data.get('total_volumes', [])
            
            if not prices:
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
            
            return df
        except Exception as e:
            logger.error(f"Failed to format market data: {e}")
            return None
    
    def is_kraken_tradeable(self, coin_id: str, symbol: str) -> tuple[bool, Optional[str], Optional[str]]:
        """Check if coin is tradeable on Kraken using reliable mapping
        
        Returns:
            tuple: (is_tradeable, kraken_symbol, kraken_pair_name)
        """
        # First check our reliable mapping
        if coin_id in self.coingecko_kraken_map:
            kraken_info = self.coingecko_kraken_map[coin_id]
            kraken_symbol = kraken_info['kraken_symbol']
            pair_name = kraken_info['pair_name']
            logger.debug(f"Found {coin_id} on Kraken as {kraken_symbol} (pair: {pair_name})")
            return True, kraken_symbol, pair_name
        
        return False, None, None
    
    def get_kraken_info(self, coin_id: str) -> Optional[dict]:
        """Get full Kraken information for a CoinGecko coin ID"""
        if coin_id in self.coingecko_kraken_map:
            return self.coingecko_kraken_map[coin_id]
        return None
    
    def get_existing_data_range(self, ticker_symbol: str) -> tuple[Optional[datetime], Optional[datetime]]:
        """Get the date range of existing data for a symbol"""
        try:
            stock = self.ab.Stocks(ticker_symbol)
            quotations = stock.Quotations
            
            if quotations.Count == 0:
                return None, None
            
            # Get first and last dates
            first_quote = quotations(0)
            last_quote = quotations(quotations.Count - 1)
            
            first_date = datetime(first_quote.Date.year, first_quote.Date.month, first_quote.Date.day)
            last_date = datetime(last_quote.Date.year, last_quote.Date.month, last_quote.Date.day)
            
            return first_date, last_date
            
        except Exception as e:
            logger.debug(f"Could not get existing data range for {ticker_symbol}: {e}")
            return None, None
    
    def filter_new_data(self, df: pd.DataFrame, existing_start: Optional[datetime], 
                       existing_end: Optional[datetime]) -> pd.DataFrame:
        """Filter DataFrame to only include new data that doesn't overlap with existing data"""
        if existing_start is None or existing_end is None:
            return df
        
        # Convert DataFrame index to datetime if needed
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        
        # Filter out data that already exists
        # Keep data that's before existing start or after existing end
        new_data = df[(df.index < existing_start) | (df.index > existing_end)]
        
        return new_data
    
    def update_existing_quotations(self, stock, df: pd.DataFrame, existing_start: datetime, 
                                 existing_end: datetime) -> int:
        """Update existing quotations that might have changed"""
        updated_count = 0
        quotations = stock.Quotations
        
        # Update data within existing range (in case of corrections)
        overlap_data = df[(df.index >= existing_start) & (df.index <= existing_end)]
        
        for date, row in overlap_data.iterrows():
            dt = date.to_pydatetime()
            
            # Find existing quotation for this date
            existing_quote = None
            for i in range(quotations.Count):
                quote = quotations(i)
                quote_date = datetime(quote.Date.year, quote.Date.month, quote.Date.day)
                if quote_date == dt.date():
                    existing_quote = quote
                    break
            
            if existing_quote:
                # Update existing quotation if values have changed
                if (abs(existing_quote.Close - float(row['Close'])) > 0.0001 or
                    abs(existing_quote.Volume - float(row['Volume'])) > 0.1):
                    existing_quote.Open = float(row['Open'])
                    existing_quote.High = float(row['High'])
                    existing_quote.Low = float(row['Low'])
                    existing_quote.Close = float(row['Close'])
                    existing_quote.Volume = float(row['Volume'])
                    
                    try:
                        existing_quote.SetExtraData('MarketCap', float(row['MarketCap']))
                    except AttributeError:
                        pass
                    
                    updated_count += 1
        
        return updated_count
    
    def import_to_amibroker(self, df: pd.DataFrame, symbol: str, name: str, coin_id: str, 
                           is_kraken: bool, kraken_info: Optional[dict] = None, 
                           force_full_update: bool = False):
        """Import or update data in AmiBroker"""
        try:
            # Determine the ticker symbol and display name
            if is_kraken and kraken_info:
                ticker_symbol = kraken_info.get('pair_name', symbol)
                display_name = f"{kraken_info.get('pair_name', symbol)} - {name}"
                kraken_symbol = kraken_info.get('kraken_symbol', '')
                alt_name = kraken_info.get('alt_name', '')
            else:
                ticker_symbol = symbol
                display_name = f"{symbol} - {name}"
                kraken_symbol = None
                alt_name = ''
            
            # Check for existing data
            existing_start, existing_end = self.get_existing_data_range(ticker_symbol)
            is_new_symbol = existing_start is None
            
            # Create or get stock object
            stock = self.ab.Stocks(ticker_symbol)
            
            # Set stock properties (always update these)
            stock.FullName = display_name
            stock.MarketID = 1
            stock.GroupID = 253 if is_kraken else 254
            
            # Add/update custom fields with comprehensive metadata
            try:
                stock.SetExtraData('CoinGeckoID', coin_id)
                stock.SetExtraData('OriginalSymbol', symbol)
                stock.SetExtraData('OriginalName', name)
                stock.SetExtraData('Kraken', 1 if is_kraken else 0)
                stock.SetExtraData('LastUpdated', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                
                if is_kraken and kraken_info:
                    stock.SetExtraData('KrakenSymbol', kraken_symbol)
                    stock.SetExtraData('KrakenPairName', kraken_info.get('pair_name', ''))
                    stock.SetExtraData('KrakenBase', kraken_info.get('base', ''))
                    stock.SetExtraData('KrakenTarget', kraken_info.get('target', ''))
                    if alt_name:
                        stock.SetExtraData('KrakenAltName', alt_name)
                        
            except AttributeError:
                logger.debug("SetExtraData not available, skipping metadata")
            
            quotations = stock.Quotations
            new_records = 0
            updated_records = 0
            
            if is_new_symbol or force_full_update:
                # Import all data for new symbols or forced updates
                logger.info(f"{'Creating new symbol' if is_new_symbol else 'Force updating'} {ticker_symbol}")
                
                if force_full_update and not is_new_symbol:
                    # Clear existing data
                    quotations.Clear()
                
                for date, row in df.iterrows():
                    dt = date.to_pydatetime()
                    quote = quotations.Add(dt)
                    quote.Open = float(row['Open'])
                    quote.High = float(row['High'])
                    quote.Low = float(row['Low'])
                    quote.Close = float(row['Close'])
                    quote.Volume = float(row['Volume'])
                    
                    try:
                        quote.SetExtraData('MarketCap', float(row['MarketCap']))
                    except AttributeError:
                        pass
                
                new_records = len(df)
                
            else:
                # Update existing symbol with new data only
                logger.info(f"Updating existing symbol {ticker_symbol} (existing: {existing_start.date()} to {existing_end.date()})")
                
                # Update overlapping data (corrections)
                updated_records = self.update_existing_quotations(stock, df, existing_start, existing_end)
                
                # Add only new data
                new_data = self.filter_new_data(df, existing_start, existing_end)
                
                for date, row in new_data.iterrows():
                    dt = date.to_pydatetime()
                    quote = quotations.Add(dt)
                    quote.Open = float(row['Open'])
                    quote.High = float(row['High'])
                    quote.Low = float(row['Low'])
                    quote.Close = float(row['Close'])
                    quote.Volume = float(row['Volume'])
                    
                    try:
                        quote.SetExtraData('MarketCap', float(row['MarketCap']))
                    except AttributeError:
                        pass
                
                new_records = len(new_data)
            
            # Save the stock
            stock.Save()
            
            # Enhanced logging
            status_parts = []
            if new_records > 0:
                status_parts.append(f"{new_records} new")
            if updated_records > 0:
                status_parts.append(f"{updated_records} updated")
            if not status_parts:
                status_parts.append("no changes")
            
            status_str = ", ".join(status_parts)
            
            if is_kraken and kraken_info:
                pair_name = kraken_info.get('pair_name', ticker_symbol)
                logger.info(f"Processed {ticker_symbol} (Kraken: {pair_name}) - {status_str}")
            else:
                logger.info(f"Processed {ticker_symbol} - {status_str}")
            
            return new_records, updated_records
            
        except Exception as e:
            logger.error(f"Failed to import/update {symbol} in AmiBroker: {e}")
            return 0, 0
    
    def save_mapping_to_file(self, filename: Optional[str] = None):
        """Save the CoinGecko-Kraken mapping to a file for reference"""
        if filename is None:
            filename = self.config.get('MAPPING', 'mapping_file')
        
        try:
            with open(filename, 'w') as f:
                json.dump(self.coingecko_kraken_map, f, indent=2)
            logger.info(f"Saved mapping to {filename}")
        except Exception as e:
            logger.error(f"Failed to save mapping: {e}")
    
    def load_mapping_from_file(self, filename: Optional[str] = None) -> bool:
        """Load previously saved mapping from file"""
        if filename is None:
            filename = self.config.get('MAPPING', 'mapping_file')
        
        try:
            with open(filename, 'r') as f:
                self.coingecko_kraken_map = json.load(f)
            logger.info(f"Loaded mapping from {filename} ({len(self.coingecko_kraken_map)} entries)")
            return True
        except FileNotFoundError:
            logger.info(f"Mapping file {filename} not found, will build new mapping")
            return False
        except Exception as e:
            logger.error(f"Failed to load mapping: {e}")
            return False
    def create_amibroker_groups(self):
        """Create AmiBroker groups for organization"""
        try:
            # Create groups for Kraken and non-Kraken coins
            groups = self.ab.Groups
            
            # Group 253 for Kraken tradeable
            kraken_group = groups(253)
            kraken_group.Name = "Crypto - Kraken Tradeable"
            
            # Group 254 for non-Kraken
            non_kraken_group = groups(254)
            non_kraken_group.Name = "Crypto - Other Exchanges"
            
            logger.info("Created AmiBroker groups")
        except Exception as e:
            logger.error(f"Failed to create AmiBroker groups: {e}")
    
    def print_kraken_mapping_stats(self):
        """Print statistics about the Kraken mapping"""
        total_mapped = len(self.coingecko_kraken_map)
        
        if total_mapped == 0:
            logger.info("No Kraken mappings found")
            return
        
        logger.info(f"Kraken Mapping Statistics:")
        logger.info(f"  Total mapped coins: {total_mapped}")
        
        # Group by target currency
        target_counts = {}
        for coin_id, info in self.coingecko_kraken_map.items():
            target = info.get('target', 'Unknown')
            target_counts[target] = target_counts.get(target, 0) + 1
        
        logger.info("  Trading pairs by quote currency:")
        for target, count in sorted(target_counts.items()):
            logger.info(f"    {target}: {count} pairs")
        
        # Show some examples with enhanced information
        logger.info("  Sample mappings:")
        for i, (coin_id, info) in enumerate(list(self.coingecko_kraken_map.items())[:5]):
            pair_name = info.get('pair_name', info.get('kraken_symbol', 'Unknown'))
            base_target = f"{info.get('base', '')}/{info.get('target', '')}"
            logger.info(f"    {coin_id} -> {pair_name} ({base_target})")
        
        if total_mapped > 5:
            logger.info(f"    ... and {total_mapped - 5} more")
    
    def run_import(self, max_coins: Optional[int] = None, min_market_cap: Optional[float] = None, 
                   force_full_update: Optional[bool] = None):
        """Main import function using configuration settings"""
        logger.info("Starting CoinGecko to AmiBroker import")
        
        # Use config values if parameters not provided
        if max_coins is None:
            max_coins = self.config.getint('IMPORT', 'max_coins')
            if max_coins == 0:
                max_coins = None
                
        if min_market_cap is None:
            min_market_cap = self.config.getfloat('IMPORT', 'min_market_cap')
            
        if force_full_update is None:
            force_full_update = self.config.getboolean('IMPORT', 'force_full_update')
        
        # Determine if we should use cached mapping
        use_cached_mapping = self.config.getboolean('MAPPING', 'use_cached_mapping')
        
        # Load or build mapping
        if use_cached_mapping and not self._should_rebuild_mapping() and self.load_mapping_from_file():
            logger.info("Using cached Kraken mapping")
        else:
            logger.info("Building new Kraken mapping (this may take a while...)")
            self._build_coingecko_kraken_mapping()
            # Save mapping for future use
            self.save_mapping_to_file()
        
        # Print mapping statistics
        self.print_kraken_mapping_stats()
        
        # Create groups
        self.create_amibroker_groups()
        
        # Get all coins
        all_coins = self.get_all_coins()
        if not all_coins:
            logger.error("No coins retrieved from CoinGecko")
            return
        
        # Apply filters
        filtered_coins = [coin for coin in all_coins if self._apply_filters(coin)]
        logger.info(f"Filtered {len(all_coins)} coins to {len(filtered_coins)} based on configuration")
        
        # Limit coins if specified
        if max_coins and max_coins > 0:
            filtered_coins = filtered_coins[:max_coins]
            logger.info(f"Limited to {len(filtered_coins)} coins")
        
        imported_count = 0
        failed_count = 0
        kraken_count = 0
        new_records_total = 0
        updated_records_total = 0
        skipped_count = 0
        
        for i, coin in enumerate(filtered_coins):
            coin_id = coin['id']
            symbol = coin['symbol'].upper()
            name = coin['name']
            
            logger.info(f"Processing {i+1}/{len(filtered_coins)}: {symbol} ({name})")
            
            try:
                # Get market data
                market_data = self.get_coin_market_data(coin_id)
                if not market_data:
                    failed_count += 1
                    continue
                
                # Format data
                df = self.format_market_data(market_data, coin)
                if df is None or df.empty:
                    logger.warning(f"No data available for {symbol}")
                    failed_count += 1
                    continue
                
                # Filter by market cap if specified
                if min_market_cap > 0:
                    latest_market_cap = df['MarketCap'].iloc[-1]
                    if latest_market_cap < min_market_cap:
                        logger.info(f"Skipping {symbol} - market cap too low: ${latest_market_cap:,.0f}")
                        skipped_count += 1
                        continue
                
                # Filter by volume if specified
                min_volume = self.config.getfloat('FILTERING', 'min_volume_24h')
                if min_volume > 0:
                    latest_volume = df['Volume'].iloc[-1]
                    if latest_volume < min_volume:
                        logger.info(f"Skipping {symbol} - volume too low: ${latest_volume:,.0f}")
                        skipped_count += 1
                        continue
                
                # Check if tradeable on Kraken
                is_kraken, kraken_symbol, kraken_pair_name = self.is_kraken_tradeable(coin_id, symbol)
                kraken_info = self.get_kraken_info(coin_id) if is_kraken else None
                
                if is_kraken:
                    kraken_count += 1
                
                # Import/update to AmiBroker
                new_records, updated_records = self.import_to_amibroker(
                    df, symbol, name, coin_id, is_kraken, kraken_info, force_full_update
                )
                
                new_records_total += new_records
                updated_records_total += updated_records
                imported_count += 1
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
                
            except Exception as e:
                logger.error(f"Failed to process {symbol}: {e}")
                failed_count += 1
                continue
        
        logger.info(f"Import completed:")
        logger.info(f"  Total processed: {imported_count}")
        logger.info(f"  New records added: {new_records_total}")
        logger.info(f"  Records updated: {updated_records_total}")
        logger.info(f"  Kraken tradeable: {kraken_count}")
        logger.info(f"  Other exchanges: {imported_count - kraken_count}")
        logger.info(f"  Skipped (filters): {skipped_count}")
        logger.info(f"  Failed: {failed_count}")
        
        # Auto-update if enabled
        if self.config.getboolean('UPDATES', 'update_on_startup'):
            logger.info("Running post-import update as configured")
            self.update_existing_data()
        
        # Create groups
        self.create_amibroker_groups()
        
        # Get all coins
        all_coins = self.get_all_coins()
        if not all_coins:
            logger.error("No coins retrieved from CoinGecko")
            return
        
        # Limit coins if specified
        if max_coins:
            all_coins = all_coins[:max_coins]
        
        imported_count = 0
        failed_count = 0
        kraken_count = 0
        new_records_total = 0
        updated_records_total = 0
        skipped_count = 0
        
        for i, coin in enumerate(all_coins):
            coin_id = coin['id']
            symbol = coin['symbol'].upper()
            name = coin['name']
            
            logger.info(f"Processing {i+1}/{len(all_coins)}: {symbol} ({name})")
            
            try:
                # Get market data
                market_data = self.get_coin_market_data(coin_id)
                if not market_data:
                    failed_count += 1
                    continue
                
                # Format data
                df = self.format_market_data(market_data, coin)
                if df is None or df.empty:
                    logger.warning(f"No data available for {symbol}")
                    failed_count += 1
                    continue
                
                # Filter by market cap if specified
                if min_market_cap > 0:
                    latest_market_cap = df['MarketCap'].iloc[-1]
                    if latest_market_cap < min_market_cap:
                        logger.info(f"Skipping {symbol} - market cap too low: ${latest_market_cap:,.0f}")
                        skipped_count += 1
                        continue
                
                # Check if tradeable on Kraken
                is_kraken, kraken_symbol, kraken_pair_name = self.is_kraken_tradeable(coin_id, symbol)
                kraken_info = self.get_kraken_info(coin_id) if is_kraken else None
                
                if is_kraken:
                    kraken_count += 1
                
                # Import/update to AmiBroker
                new_records, updated_records = self.import_to_amibroker(
                    df, symbol, name, coin_id, is_kraken, kraken_info, force_full_update
                )
                
                new_records_total += new_records
                updated_records_total += updated_records
                imported_count += 1
                
                # Rate limiting - CoinGecko allows 10-50 calls per minute for free tier
                time.sleep(1.5)  # 40 calls per minute
                
            except Exception as e:
                logger.error(f"Failed to process {symbol}: {e}")
                failed_count += 1
                continue
        
        logger.info(f"Import completed:")
        logger.info(f"  Total processed: {imported_count}")
        logger.info(f"  New records added: {new_records_total}")
        logger.info(f"  Records updated: {updated_records_total}")
        logger.info(f"  Kraken tradeable: {kraken_count}")
        logger.info(f"  Other exchanges: {imported_count - kraken_count}")
        logger.info(f"  Skipped (market cap): {skipped_count}")
        logger.info(f"  Failed: {failed_count}")
    
    def update_existing_data(self, days_back: int = 7, symbols: Optional[List[str]] = None):
        """Update existing data with recent prices for specific symbols or all symbols"""
        logger.info(f"Updating existing data for last {days_back} days")
        
        try:
            stocks = self.ab.Stocks
            updated_count = 0
            failed_count = 0
            
            # If specific symbols provided, filter to those
            if symbols:
                symbol_set = set(symbols)
                stocks_to_update = []
                for i in range(stocks.Count):
                    stock = stocks(i)
                    if stock.Ticker in symbol_set:
                        stocks_to_update.append(stock)
            else:
                stocks_to_update = [stocks(i) for i in range(stocks.Count)]
            
            logger.info(f"Updating {len(stocks_to_update)} symbols")
            
            for i, stock in enumerate(stocks_to_update):
                ticker = stock.Ticker
                
                if not ticker:
                    continue
                
                logger.info(f"Updating {i+1}/{len(stocks_to_update)}: {ticker}")
                
                try:
                    # Get CoinGecko ID from metadata
                    coin_id = None
                    try:
                        coin_id = stock.GetExtraData('CoinGeckoID')
                    except:
                        # Fallback to using ticker as coin_id (simplified approach)
                        coin_id = ticker.lower()
                    
                    if not coin_id:
                        logger.warning(f"No CoinGecko ID found for {ticker}, skipping")
                        continue
                    
                    # Get recent market data
                    market_data = self.get_coin_market_data(coin_id, days_back)
                    if not market_data:
                        failed_count += 1
                        continue
                    
                    # Format data
                    df = self.format_market_data(market_data, {'id': coin_id, 'symbol': ticker, 'name': stock.FullName})
                    if df is None or df.empty:
                        logger.warning(f"No recent data available for {ticker}")
                        failed_count += 1
                        continue
                    
                    # Get existing data range
                    existing_start, existing_end = self.get_existing_data_range(ticker)
                    
                    if existing_start is None:
                        logger.warning(f"No existing data found for {ticker}, skipping update")
                        continue
                    
                    # Update quotations
                    quotations = stock.Quotations
                    new_records = 0
                    updated_records = 0
                    
                    # Update overlapping data
                    updated_records = self.update_existing_quotations(stock, df, existing_start, existing_end)
                    
                    # Add new data
                    new_data = self.filter_new_data(df, existing_start, existing_end)
                    
                    for date, row in new_data.iterrows():
                        dt = date.to_pydatetime()
                        quote = quotations.Add(dt)
                        quote.Open = float(row['Open'])
                        quote.High = float(row['High'])
                        quote.Low = float(row['Low'])
                        quote.Close = float(row['Close'])
                        quote.Volume = float(row['Volume'])
                        
                        try:
                            quote.SetExtraData('MarketCap', float(row['MarketCap']))
                        except AttributeError:
                            pass
                        
                        new_records += 1
                    
                    # Update last updated timestamp
                    try:
                        stock.SetExtraData('LastUpdated', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    except AttributeError:
                        pass
                    
                    stock.Save()
                    
                    if new_records > 0 or updated_records > 0:
                        logger.info(f"Updated {ticker}: {new_records} new, {updated_records} updated")
                        updated_count += 1
                    else:
                        logger.info(f"No updates needed for {ticker}")
                    
                    time.sleep(1.5)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Failed to update {ticker}: {e}")
                    failed_count += 1
                    continue
            
            logger.info(f"Update completed: {updated_count} symbols updated, {failed_count} failed")
                    
        except Exception as e:
            logger.error(f"Failed to update existing data: {e}")

def main():
    """Main execution function"""
    importer = CoinGeckoAmiBrokerImporter()
    
    # Configuration
    MAX_COINS = 100  # Limit for testing, set to None for all coins
    MIN_MARKET_CAP = 10000000  # $10M minimum market cap
    USE_CACHED_MAPPING = True  # Use cached mapping if available
    FORCE_FULL_UPDATE = False  # Set to True to force complete data refresh
    
    # Run the import/update
    importer.run_import(
        max_coins=MAX_COINS, 
        min_market_cap=MIN_MARKET_CAP,
        use_cached_mapping=USE_CACHED_MAPPING,
        force_full_update=FORCE_FULL_UPDATE
    )
    
    # Optionally update existing data with recent prices
    # importer.update_existing_data(days_back=7)
    
    # Or update specific symbols only
    # importer.update_existing_data(days_back=7, symbols=['XXBTZUSD', 'XETHZUSD'])

if __name__ == "__main__":
    main()
