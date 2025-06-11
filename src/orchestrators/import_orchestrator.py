"""
Import Orchestrator - Main coordination class
Orchestrates the entire import process using all components
"""

import time
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
import logging

from configuration_manager import ConfigurationManager
from logging_manager import LoggingManager
from abstract_data_provider import AbstractDataProvider
from abstract_exchange_mapper import AbstractExchangeMapper
from abstract_database_adapter import AbstractDatabaseAdapter
from data_filter import DataFilter
from update_scheduler import UpdateScheduler

logger = logging.getLogger(__name__)


@dataclass
class ImportResult:
    """Result of an import operation"""
    total_processed: int = 0
    new_records: int = 0
    updated_records: int = 0
    failed_count: int = 0
    kraken_count: int = 0
    skipped_count: int = 0
    execution_time: float = 0.0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


@dataclass
class ProcessResult:
    """Result of processing a single coin"""
    success: bool
    new_records: int = 0
    updated_records: int = 0
    error_message: str = ""
    is_kraken: bool = False


class ImportOrchestrator:
    """Main orchestrator for the crypto data import process"""
    
    def __init__(self, config_path: Optional[str] = None):
        # Initialize configuration
        self.config = ConfigurationManager(config_path)
        
        # Setup logging
        self.logging_manager = LoggingManager(self.config)
        
        # Initialize components (will be set by factories)
        self.data_provider: Optional[AbstractDataProvider] = None
        self.exchange_mappers: List[AbstractExchangeMapper] = []
        self.database_adapter: Optional[AbstractDatabaseAdapter] = None
        self.data_filter: Optional[DataFilter] = None
        self.update_scheduler: Optional[UpdateScheduler] = None
        
        # State tracking
        self.is_initialized = False
        self.last_import_result: Optional[ImportResult] = None
    
    def initialize(self, data_provider: AbstractDataProvider,
                  exchange_mappers: List[AbstractExchangeMapper],
                  database_adapter: AbstractDatabaseAdapter) -> bool:
        """Initialize the orchestrator with required components
        
        Args:
            data_provider: Data provider implementation
            exchange_mappers: List of exchange mapper implementations  
            database_adapter: Database adapter implementation
            
        Returns:
            True if initialization successful, False otherwise
        """
        try:
            logger.info("Initializing Import Orchestrator")
            
            # Set components
            self.data_provider = data_provider
            self.exchange_mappers = exchange_mappers
            self.database_adapter = database_adapter
            
            # Initialize filter and scheduler
            self.data_filter = DataFilter(self.config)
            self.update_scheduler = UpdateScheduler(self.config)
            
            # Validate setup
            if not self.validate_setup():
                return False
            
            self.is_initialized = True
            logger.info("Import Orchestrator initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Import Orchestrator: {e}")
            return False
    
    def validate_setup(self) -> bool:
        """Validate that all required components are properly configured
        
        Returns:
            True if setup is valid, False otherwise
        """
        validation_errors = []
        
        # Check data provider
        if not self.data_provider:
            validation_errors.append("Data provider not set")
        
        # Check database adapter
        if not self.database_adapter:
            validation_errors.append("Database adapter not set")
        elif not self.database_adapter.validate_connection():
            validation_errors.append("Database connection validation failed")
        
        # Check exchange mappers
        if not self.exchange_mappers:
            logger.warning("No exchange mappers configured")
        else:
            for mapper in self.exchange_mappers:
                if not mapper.validate_mapping():
                    logger.warning(f"Exchange mapper validation failed: {mapper.get_exchange_name()}")
        
        # Check configuration
        required_configs = [
            ('DATABASE', 'database_path'),
            ('IMPORT', 'max_coins'),
            ('IMPORT', 'min_market_cap')
        ]
        
        for section, key in required_configs:
            if not self.config.get(section, key):
                validation_errors.append(f"Missing configuration: [{section}] {key}")
        
        if validation_errors:
            for error in validation_errors:
                logger.error(f"Validation error: {error}")
            return False
        
        logger.info("Setup validation successful")
        return True
    
    def run_import(self, max_coins: Optional[int] = None,
                  min_market_cap: Optional[float] = None,
                  force_full_update: Optional[bool] = None) -> ImportResult:
        """Run the complete import process
        
        Args:
            max_coins: Maximum number of coins to import
            min_market_cap: Minimum market cap filter
            force_full_update: Force complete data refresh
            
        Returns:
            ImportResult containing operation results
        """
        if not self.is_initialized:
            logger.error("Orchestrator not initialized")
            return ImportResult(errors=["Orchestrator not initialized"])
        
        start_time = time.time()
        result = ImportResult()
        
        try:
            logger.info("Starting import process")
            
            # Use config values if parameters not provided
            if max_coins is None:
                max_coins = self.config.getint('IMPORT', 'max_coins')
                if max_coins == 0:
                    max_coins = None
                    
            if min_market_cap is None:
                min_market_cap = self.config.getfloat('IMPORT', 'min_market_cap')
                
            if force_full_update is None:
                force_full_update = self.config.getboolean('IMPORT', 'force_full_update')
            
            # Setup database groups
            if not self.database_adapter.create_groups():
                logger.warning("Failed to create database groups")
            
            # Load exchange mappings
            for mapper in self.exchange_mappers:
                if not mapper.build_mapping():
                    logger.warning(f"Failed to build mapping for {mapper.get_exchange_name()}")
            
            # Get all coins from data provider
            all_coins = self.data_provider.get_all_coins()
            if not all_coins:
                logger.error("No coins retrieved from data provider")
                result.errors.append("No coins retrieved from data provider")
                return result
            
            # Apply filters
            filtered_coins = []
            for coin in all_coins:
                # Add market cap data if needed for filtering
                if min_market_cap > 0:
                    coin_details = self.data_provider.get_coin_details(coin['id'])
                    if coin_details:
                        market_data = coin_details.get('market_data', {})
                        coin['market_cap'] = market_data.get('market_cap', {}).get('usd', 0)
                
                if self.data_filter.apply_filters(coin):
                    filtered_coins.append(coin)
            
            logger.info(f"Filtered {len(all_coins)} coins to {len(filtered_coins)}")
            
            # Limit coins if specified
            if max_coins and max_coins > 0:
                filtered_coins = filtered_coins[:max_coins]
                logger.info(f"Limited to {len(filtered_coins)} coins")
            
            # Process each coin
            for i, coin in enumerate(filtered_coins):
                logger.info(f"Processing {i+1}/{len(filtered_coins)}: {coin['symbol']} ({coin['name']})")
                
                process_result = self.process_coin(coin, force_full_update)
                
                if process_result.success:
                    result.total_processed += 1
                    result.new_records += process_result.new_records
                    result.updated_records += process_result.updated_records
                    
                    if process_result.is_kraken:
                        result.kraken_count += 1
                else:
                    result.failed_count += 1
                    if process_result.error_message:
                        result.errors.append(f"{coin['symbol']}: {process_result.error_message}")
                
                # Rate limiting
                time.sleep(self.config.getfloat('IMPORT', 'rate_limit_delay', 1.5))
            
            # Auto-update if enabled
            if self.config.getboolean('UPDATES', 'update_on_startup'):
                logger.info("Running post-import update as configured")
                self.run_update()
            
        except Exception as e:
            logger.error(f"Import process failed: {e}")
            result.errors.append(f"Import process failed: {e}")
        
        finally:
            result.execution_time = time.time() - start_time
            self.last_import_result = result
            self._log_import_summary(result)
        
        return result
    
    def process_coin(self, coin: Dict, force_full_update: bool = False) -> ProcessResult:
        """Process a single coin through the complete pipeline
        
        Args:
            coin: Coin data dictionary
            force_full_update: Force complete data refresh
            
        Returns:
            ProcessResult containing operation results
        """
        coin_id = coin['id']
        symbol = coin['symbol'].upper()
        name = coin['name']
        
        try:
            # Get market data
            market_data = self.data_provider.get_market_data(coin_id)
            if not market_data:
                return ProcessResult(success=False, error_message="No market data available")
            
            # Format data
            if hasattr(self.data_provider, 'format_market_data'):
                df = self.data_provider.format_market_data(market_data, coin)
            else:
                logger.warning("Data provider does not support format_market_data")
                return ProcessResult(success=False, error_message="Data formatting not supported")
            
            if df is None or df.empty:
                return ProcessResult(success=False, error_message="No formatted data available")
            
            # Apply exchange mapping
            coin_with_exchange = self.apply_exchange_mapping(coin)
            
            # Import to database
            if self.import_coin_data(coin_with_exchange, df, force_full_update):
                new_records, updated_records = self.database_adapter.update_data(symbol, df)
                return ProcessResult(
                    success=True,
                    new_records=new_records,
                    updated_records=updated_records,
                    is_kraken=coin_with_exchange.get('is_kraken', False)
                )
            else:
                return ProcessResult(success=False, error_message="Database import failed")
                
        except Exception as e:
            logger.error(f"Failed to process {symbol}: {e}")
            return ProcessResult(success=False, error_message=str(e))
    
    def apply_exchange_mapping(self, coin: Dict) -> Dict:
        """Apply exchange mapping to coin data
        
        Args:
            coin: Original coin data
            
        Returns:
            Coin data enhanced with exchange information
        """
        enhanced_coin = coin.copy()
        exchange_info = {}
        
        # Check each exchange mapper
        for mapper in self.exchange_mappers:
            if mapper.is_tradeable(coin['id']):
                exchange_data = mapper.map_coin_to_exchange(coin['id'])
                if exchange_data:
                    exchange_name = mapper.get_exchange_name()
                    exchange_info[exchange_name] = {
                        'symbol': exchange_data.symbol,
                        'pair_name': exchange_data.pair_name,
                        'base_currency': exchange_data.base_currency,
                        'target_currency': exchange_data.target_currency,
                        'is_active': exchange_data.is_active
                    }
                    
                    # Special handling for Kraken
                    if exchange_name.lower() == 'kraken':
                        enhanced_coin['is_kraken'] = True
                        enhanced_coin['kraken_symbol'] = exchange_data.symbol
                        enhanced_coin['kraken_pair_name'] = exchange_data.pair_name
        
        enhanced_coin['exchanges'] = exchange_info
        return enhanced_coin
    
    def import_coin_data(self, coin: Dict, data, force_full_update: bool = False) -> bool:
        """Import coin data to database
        
        Args:
            coin: Coin information with exchange data
            data: Market data DataFrame
            force_full_update: Force complete data refresh
            
        Returns:
            True if import successful, False otherwise
        """
        try:
            symbol = coin['symbol'].upper()
            
            # Use Kraken pair name if available
            if coin.get('is_kraken') and coin.get('kraken_pair_name'):
                symbol = coin['kraken_pair_name']
            
            # Prepare metadata
            metadata = {
                'full_name': self._get_display_name(coin),
                'group_id': 253 if coin.get('is_kraken') else 254,
                'market_id': 1,
                'CoinGeckoID': coin['id'],
                'OriginalSymbol': coin['symbol'],
                'OriginalName': coin['name'],
                'Kraken': 1 if coin.get('is_kraken') else 0,
                'LastUpdated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Add Kraken-specific metadata
            if coin.get('is_kraken'):
                kraken_info = coin.get('exchanges', {}).get('kraken', {})
                metadata.update({
                    'KrakenSymbol': kraken_info.get('symbol', ''),
                    'KrakenPairName': kraken_info.get('pair_name', ''),
                    'KrakenBase': kraken_info.get('base_currency', ''),
                    'KrakenTarget': kraken_info.get('target_currency', '')
                })
            
            # Import or update data
            if force_full_update or not self.database_adapter.symbol_exists(symbol):
                return self.database_adapter.import_data(symbol, data, metadata)
            else:
                new_records, updated_records = self.database_adapter.update_data(symbol, data)
                return new_records > 0 or updated_records > 0
                
        except Exception as e:
            logger.error(f"Failed to import coin data: {e}")
            return False
    
    def _get_display_name(self, coin: Dict) -> str:
        """Get display name for the coin based on exchange mapping"""
        if coin.get('is_kraken') and coin.get('kraken_pair_name'):
            return f"{coin['kraken_pair_name']} - {coin['name']}"
        else:
            return f"{coin['symbol']} - {coin['name']}"
    
    def run_update(self, symbols: Optional[List[str]] = None) -> bool:
        """Run update process for existing data
        
        Args:
            symbols: Optional list of specific symbols to update
            
        Returns:
            True if update successful, False otherwise
        """
        if not self.is_initialized:
            logger.error("Orchestrator not initialized")
            return False
        
        try:
            days_back = self.config.getint('UPDATES', 'update_days_back', 7)
            
            if symbols:
                logger.info(f"Updating {len(symbols)} specific symbols")
            else:
                symbols = self.database_adapter.get_symbol_list()
                logger.info(f"Updating all {len(symbols)} symbols")
            
            updated_count = 0
            failed_count = 0
            
            for symbol in symbols:
                try:
                    # Get coin ID from metadata
                    metadata = self.database_adapter.get_symbol_metadata(symbol)
                    coin_id = metadata.get('CoinGeckoID') if metadata else symbol.lower()
                    
                    # Get recent market data
                    market_data = self.data_provider.get_market_data(coin_id, days_back)
                    if market_data:
                        if hasattr(self.data_provider, 'format_market_data'):
                            df = self.data_provider.format_market_data(market_data, {'id': coin_id, 'symbol': symbol})
                            if df is not None and not df.empty:
                                new_records, updated_records = self.database_adapter.update_data(symbol, df)
                                if new_records > 0 or updated_records > 0:
                                    updated_count += 1
                                    logger.info(f"Updated {symbol}: {new_records} new, {updated_records} updated")
                    
                    # Rate limiting
                    time.sleep(self.config.getfloat('IMPORT', 'rate_limit_delay', 1.5))
                    
                except Exception as e:
                    logger.error(f"Failed to update {symbol}: {e}")
                    failed_count += 1
            
            logger.info(f"Update completed: {updated_count} updated, {failed_count} failed")
            return True
            
        except Exception as e:
            logger.error(f"Update process failed: {e}")
            return False
    
    def _log_import_summary(self, result: ImportResult):
        """Log summary of import results"""
        logger.info("Import Summary:")
        logger.info(f"  Total processed: {result.total_processed}")
        logger.info(f"  New records: {result.new_records}")
        logger.info(f"  Updated records: {result.updated_records}")
        logger.info(f"  Kraken tradeable: {result.kraken_count}")
        logger.info(f"  Other exchanges: {result.total_processed - result.kraken_count}")
        logger.info(f"  Skipped: {result.skipped_count}")
        logger.info(f"  Failed: {result.failed_count}")
        logger.info(f"  Execution time: {result.execution_time:.2f} seconds")
        
        if result.errors:
            logger.warning(f"Errors encountered: {len(result.errors)}")
            for error in result.errors[:5]:  # Show first 5 errors
                logger.warning(f"  {error}")
            if len(result.errors) > 5:
                logger.warning(f"  ... and {len(result.errors) - 5} more errors")
    
    def get_status(self) -> Dict:
        """Get current status of the orchestrator
        
        Returns:
            Dictionary containing status information
        """
        status = {
            'initialized': self.is_initialized,
            'components': {
                'data_provider': type(self.data_provider).__name__ if self.data_provider else None,
                'exchange_mappers': [type(mapper).__name__ for mapper in self.exchange_mappers],
                'database_adapter': type(self.database_adapter).__name__ if self.database_adapter else None,
                'data_filter': self.data_filter is not None,
                'update_scheduler': self.update_scheduler is not None
            },
            'last_import': None,
            'update_status': None
        }
        
        if self.last_import_result:
            status['last_import'] = {
                'total_processed': self.last_import_result.total_processed,
                'execution_time': self.last_import_result.execution_time,
                'errors_count': len(self.last_import_result.errors)
            }
        
        if self.update_scheduler:
            status['update_status'] = self.update_scheduler.get_update_stats()
        
        return status
    
    def cleanup(self):
        """Cleanup resources and connections"""
        try:
            logger.info("Cleaning up Import Orchestrator")
            
            # Cleanup components
            if self.database_adapter:
                # Database adapters might need cleanup
                pass
            
            if self.update_scheduler:
                self.update_scheduler.cleanup_old_state()
            
            self.is_initialized = False
            logger.info("Cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def get_performance_metrics(self) -> Dict:
        """Get performance metrics for the import process
        
        Returns:
            Dictionary containing performance metrics
        """
        metrics = {
            'last_import_time': None,
            'average_processing_time': None,
            'records_per_second': None,
            'error_rate': None
        }
        
        if self.last_import_result:
            result = self.last_import_result
            metrics['last_import_time'] = result.execution_time
            
            if result.total_processed > 0:
                metrics['average_processing_time'] = result.execution_time / result.total_processed
                metrics['records_per_second'] = result.new_records / result.execution_time if result.execution_time > 0 else 0
                metrics['error_rate'] = result.failed_count / (result.total_processed + result.failed_count)
        
        return metrics