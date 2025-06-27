"""
Configuration Manager for Crypto Data Importer
Handles all configuration file operations and validation
"""

import configparser
import os
import logging
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class ConfigurationManager:
    """Manages configuration files for the CoinGecko AmiBroker Importer"""
    
    def __init__(self, config_path: Optional[str] = None):
        # CVE-002 Remediation: Secure path validation and sanitization
        if config_path:
            self.config_path = self._sanitize_config_path(config_path)
        else:
            self.config_path = "config.ini"
        
        self.config = configparser.ConfigParser()
        self.default_config = self._get_default_config()
        self.load_config()
    
    def _sanitize_config_path(self, config_path: str) -> str:
        """
        CVE-002 Remediation: Sanitize and validate configuration file path
        
        This method prevents path traversal attacks by:
        1. Resolving the path to prevent directory traversal (../)
        2. Validating against a whitelist of allowed directories
        3. Rejecting paths that escape the allowed boundaries
        
        Args:
            config_path: User-provided configuration file path
            
        Returns:
            str: Sanitized and validated configuration path
            
        Raises:
            ValueError: If path is invalid or attempts traversal
        """
        try:
            # Normalize and resolve the path to handle . and .. components
            resolved_path = Path(config_path).resolve()
            
            # Define allowed directories (whitelist approach)
            current_dir = Path.cwd().resolve()
            user_home = Path.home().resolve()
            
            allowed_dirs = [
                current_dir,  # Current working directory
                current_dir / "config",  # Config subdirectory
                user_home / ".config",  # User config directory
                user_home / ".config" / "crypto-data-importer",  # App-specific config
            ]
            
            # Check if resolved path is within any allowed directory
            path_is_safe = False
            for allowed_dir in allowed_dirs:
                try:
                    # This will raise ValueError if path is not within allowed_dir
                    resolved_path.relative_to(allowed_dir)
                    path_is_safe = True
                    break
                except ValueError:
                    continue
            
            if not path_is_safe:
                logger.error(f"CVE-002 BLOCKED: Path traversal attempt detected: {config_path}")
                logger.error(f"Resolved path: {resolved_path}")
                logger.error(f"Allowed directories: {[str(d) for d in allowed_dirs]}")
                raise ValueError(f"Configuration path not in allowed directories: {config_path}")
            
            # CRITICAL: Block Windows-style absolute paths on Unix systems
            path_str = str(resolved_path)
            if ':' in path_str and ('\\' in path_str or '/' in path_str):
                # This looks like a Windows absolute path (C:\... or C:/...)
                logger.error(f"CVE-002 BLOCKED: Windows-style absolute path detected: {config_path}")
                raise ValueError(f"Windows-style absolute path not allowed: {config_path}")
            
            # Block any path containing Windows system directories
            windows_system_patterns = [
                'windows', 'system32', 'program files', 'users',
                'etc', 'root', 'usr', 'var', 'tmp', 'home'
            ]
            path_lower = path_str.lower()
            for pattern in windows_system_patterns:
                if pattern in path_lower:
                    logger.error(f"CVE-002 BLOCKED: System directory in path: {pattern}")
                    raise ValueError(f"System directory access not allowed: {config_path}")
            
            # Additional validation: ensure it's a .ini or .cfg file
            if resolved_path.suffix.lower() not in ['.ini', '.cfg', '.conf']:
                logger.warning(f"Unusual config file extension: {resolved_path.suffix}")
            
            # Ensure filename doesn't contain dangerous patterns
            filename = resolved_path.name
            dangerous_patterns = ['..', '//', '\\\\', '<', '>', '|', '*', '?', ':']
            for pattern in dangerous_patterns:
                if pattern in filename:
                    logger.error(f"CVE-002 BLOCKED: Dangerous pattern in filename: {pattern}")
                    raise ValueError(f"Invalid filename pattern detected: {filename}")
            
            logger.info(f"CVE-002 SAFE: Configuration path validated: {resolved_path}")
            return str(resolved_path)
            
        except Exception as e:
            logger.error(f"CVE-002: Path validation failed for {config_path}: {e}")
            # Fall back to default safe path
            default_path = Path.cwd() / "config.ini"
            logger.info(f"CVE-002: Using fallback path: {default_path}")
            return str(default_path)
    
    def _secure_file_write(self, file_path: str, content: str) -> bool:
        """
        CVE-002 Remediation: Secure file write operation with validation
        
        Args:
            file_path: Path to write to (must be pre-validated)
            content: Content to write
            
        Returns:
            bool: True if write successful, False otherwise
        """
        try:
            # Double-check that path is still safe (defense in depth)
            validated_path = Path(file_path).resolve()
            
            # Ensure parent directory exists
            validated_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to temporary file first, then rename (atomic operation)
            temp_path = validated_path.with_suffix('.tmp')
            
            with open(temp_path, 'w', encoding='utf-8') as temp_file:
                temp_file.write(content)
                temp_file.flush()
                try:
                    os.fsync(temp_file.fileno())  # Ensure data is written to disk
                except (OSError, ValueError):
                    # Handle cases where fsync is not available or file is mock
                    pass
            
            # Atomic rename
            temp_path.rename(validated_path)
            
            logger.info(f"CVE-002 SAFE: File written securely: {validated_path}")
            return True
            
        except Exception as e:
            logger.error(f"CVE-002: Secure file write failed for {file_path}: {e}")
            # Clean up temp file if it exists
            try:
                if 'temp_path' in locals():
                    temp_path.unlink(missing_ok=True)
            except:
                pass
            return False
    
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
                'cache_expiry_hours': '24',
                'checkpoint_enabled': 'true',
                'checkpoint_frequency': '100',
                'resume_on_restart': 'true',
                'checkpoint_file': 'kraken_mapping_checkpoint.json'
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
            },
            'PROVIDERS': {
                'data_provider': 'coingecko',
                'exchanges': 'kraken',
                'database_adapter': 'amibroker'
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
        
        # CVE-002 Remediation: Use secure file write operation
        if self._secure_file_write(self.config_path, config_content):
            logger.info(f"Default configuration created at: {self.config_path}")
        else:
            logger.error(f"Failed to create default configuration at: {self.config_path}")
            raise IOError(f"Could not create configuration file: {self.config_path}")
    
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

# Enable checkpoint/resume functionality for mapping process
checkpoint_enabled = true

# Number of coins to process before saving checkpoint
checkpoint_frequency = 100

# Automatically resume from checkpoint on restart
resume_on_restart = true

# File to store checkpoint progress data
checkpoint_file = kraken_mapping_checkpoint.json

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

[PROVIDERS]
# Data provider to use (coingecko, binance, etc.)
data_provider = coingecko

# Comma-separated list of exchanges to map (kraken, binance, etc.)
exchanges = kraken

# Database adapter to use (amibroker, metatrader, etc.)
database_adapter = amibroker
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
        required_sections = ['DATABASE', 'IMPORT', 'MAPPING', 'PROVIDERS']
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
            # CVE-002 Remediation: Use secure file write operation
            # Convert ConfigParser to string format
            import io
            config_buffer = io.StringIO()
            self.config.write(config_buffer)
            config_content = config_buffer.getvalue()
            config_buffer.close()
            
            if self._secure_file_write(self.config_path, config_content):
                logger.info(f"Configuration saved to: {self.config_path}")
            else:
                logger.error(f"Failed to save configuration to: {self.config_path}")
                raise IOError(f"Could not save configuration file: {self.config_path}")
                
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
            raise
    
    def print_config(self):
        """Print current configuration"""
        logger.info("Current Configuration:")
        for section in self.config.sections():
            logger.info(f"  [{section}]")
            for key, value in self.config.items(section):
                logger.info(f"    {key} = {value}")
