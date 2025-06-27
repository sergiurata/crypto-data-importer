"""
Kraken Exchange Mapper Implementation
Handles mapping between CoinGecko coins and Kraken trading pairs
"""

import json
import os
import requests
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from .abstract_exchange_mapper import AbstractExchangeMapper, ExchangeInfo
from providers.abstract_data_provider import AbstractDataProvider

logger = logging.getLogger(__name__)


class KrakenMapper(AbstractExchangeMapper):
    """Kraken-specific implementation of AbstractExchangeMapper"""
    
    def __init__(self, config):
        super().__init__(config)
        self.kraken_api_url = "https://api.kraken.com/0/public"
        self.asset_pairs = {}
        self.assets = {}
        self.session = requests.Session()
        
        # Cache settings
        self.cache_file = self.config.get('MAPPING', 'mapping_file', 'kraken_mapping.json')
        self.cache_expiry_hours = self.config.getint('MAPPING', 'cache_expiry_hours', 24)
        
        # Checkpoint settings
        self.checkpoint_enabled = self.config.getboolean('MAPPING', 'checkpoint_enabled', True)
        self.checkpoint_frequency = self.config.getint('MAPPING', 'checkpoint_frequency', 100)
        self.resume_on_restart = self.config.getboolean('MAPPING', 'resume_on_restart', True)
        self.checkpoint_file = self.config.get('MAPPING', 'checkpoint_file', 'kraken_mapping_checkpoint.json')
        
        # Retry settings
        self.retry_failed_coins = self.config.getboolean('MAPPING', 'retry_failed_coins', True)
        self.max_retry_attempts = self.config.getint('MAPPING', 'max_retry_attempts', 3)
        self.retry_counts = {}  # Track retry attempts per coin
    
    def get_exchange_name(self) -> str:
        """Get the name of this exchange"""
        return "kraken"
    
    def load_exchange_data(self) -> bool:
        """Load Kraken assets and trading pairs"""
        try:
            # Load assets
            if not self._load_kraken_assets():
                logger.error("Failed to load Kraken assets")
                return False
            
            # Load asset pairs
            if not self._load_kraken_pairs():
                logger.error("Failed to load Kraken asset pairs")
                return False
            
            logger.info(f"Loaded {len(self.assets)} assets and {len(self.asset_pairs)} pairs from Kraken")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load Kraken exchange data: {e}")
            return False
    
    def _load_kraken_assets(self) -> bool:
        """Load Kraken assets from API"""
        try:
            response = self.make_api_request(f"{self.kraken_api_url}/Assets")
            
            data = response.json()
            if data.get('error'):
                logger.error(f"Kraken API error (assets): {data.get('error')}")
                return False
            
            self.assets = data.get('result', {})
            return True
            
        except Exception as e:
            logger.error(f"Failed to load Kraken assets: {e}")
            return False
    
    def _load_kraken_pairs(self) -> bool:
        """Load Kraken asset pairs from API"""
        try:
            response = self.make_api_request(f"{self.kraken_api_url}/AssetPairs")
            
            data = response.json()
            if data.get('error'):
                logger.error(f"Kraken API error (pairs): {data.get('error')}")
                return False
            
            self.asset_pairs = data.get('result', {})
            return True
            
        except Exception as e:
            logger.error(f"Failed to load Kraken asset pairs: {e}")
            return False
    
    def map_coin_to_exchange(self, coin_id: str) -> Optional[ExchangeInfo]:
        """Map a CoinGecko coin ID to Kraken exchange information"""
        if coin_id in self.mapping_cache:
            cached_data = self.mapping_cache[coin_id]
            return ExchangeInfo(**cached_data)
        return None
    
    def is_tradeable(self, coin_id: str) -> bool:
        """Check if a coin is tradeable on Kraken"""
        return coin_id in self.mapping_cache
    
    def get_symbol_mapping(self, coin_id: str) -> Optional[str]:
        """Get Kraken-specific symbol for a coin"""
        exchange_info = self.map_coin_to_exchange(coin_id)
        return exchange_info.pair_name if exchange_info else None
    
    def _build_coin_mapping(self, data_provider: AbstractDataProvider) -> Dict:
        """Build mapping between CoinGecko IDs and Kraken data using CoinGecko API with checkpoint/resume support"""
        try:
            # Get all coins from CoinGecko
            all_coins = data_provider.get_all_coins()
            total_coins = len(all_coins)
            
            # Check if we should resume from checkpoint
            if self._should_resume():
                checkpoint_data = self._load_checkpoint()
                resume_index, processed_coin_ids, mapping = self._get_resume_point(all_coins)
                failed_coin_ids = checkpoint_data.get('failed_coin_ids', []) if checkpoint_data else []
                self.retry_counts = checkpoint_data.get('retry_counts', {}) if checkpoint_data else {}
                start_time = None  # Will be loaded from checkpoint
                logger.info(f"Resuming mapping process from coin {resume_index + 1}/{total_coins}")
                logger.info(f"Loaded {len(failed_coin_ids)} failed coins and {len(self.retry_counts)} retry counts from checkpoint")
            else:
                checkpoint_data = None
                resume_index = 0
                processed_coin_ids = []
                mapping = {}
                failed_coin_ids = []
                self.retry_counts = {}
                start_time = datetime.now()
                logger.info(f"Starting fresh mapping process for {total_coins} coins")
            
            mapped_count = len(mapping)
            batch_size = 50
            
            # Process coins starting from resume point
            for i in range(resume_index, total_coins):
                coin = all_coins[i]
                coin_id = coin['id']
                
                # Skip if already processed (shouldn't happen with proper resume logic, but safety check)
                if coin_id in processed_coin_ids:
                    continue
                
                # Check if this coin previously failed and implement smart retry logic
                if coin_id in failed_coin_ids:
                    if self.retry_failed_coins:
                        # Check retry attempts from checkpoint if available
                        retry_count = self._get_retry_count(coin_id, checkpoint_data if 'checkpoint_data' in locals() else None)
                        if retry_count < self.max_retry_attempts:
                            logger.debug(f"Retrying previously failed coin: {coin_id} (attempt {retry_count + 1}/{self.max_retry_attempts})")
                            failed_coin_ids.remove(coin_id)  # Remove from failed list to retry
                        else:
                            logger.debug(f"Coin {coin_id} exceeded max retry attempts ({self.max_retry_attempts}), permanently skipping")
                            processed_coin_ids.append(coin_id)  # Mark as processed to skip permanently
                            continue
                    else:
                        logger.debug(f"Retry disabled, skipping previously failed coin: {coin_id}")
                        processed_coin_ids.append(coin_id)  # Mark as processed to skip permanently
                        continue
                
                try:
                    # Get exchange data for this coin
                    exchange_data = data_provider.get_exchange_data(coin_id)
                    
                    if exchange_data:
                        kraken_info = self._extract_kraken_info(exchange_data)
                        if kraken_info:
                            mapping[coin_id] = kraken_info
                            mapped_count += 1
                    
                    # Add to processed list
                    processed_coin_ids.append(coin_id)
                    
                    # Rate limiting
                    time.sleep(self.config.getfloat('IMPORT', 'rate_limit_delay', 1.5))
                    
                except Exception as e:
                    logger.debug(f"Failed to process {coin_id}: {e}")
                    # Track retry attempt
                    retry_count = self._get_retry_count(coin_id, checkpoint_data if 'checkpoint_data' in locals() else None)
                    self._update_retry_count(coin_id, retry_count + 1)
                    
                    if coin_id not in failed_coin_ids:
                        failed_coin_ids.append(coin_id)
                    
                    # DO NOT mark as processed unless max retries exceeded
                    if retry_count + 1 >= self.max_retry_attempts:
                        logger.debug(f"Coin {coin_id} exceeded max retry attempts ({self.max_retry_attempts}), marking as processed")
                        processed_coin_ids.append(coin_id)
                    
                    continue
                
                # Progress reporting (based on coins attempted, not just processed)
                attempted_count = i + 1
                if attempted_count % 10 == 0 or attempted_count == total_coins:
                    progress_pct = (attempted_count / total_coins) * 100
                    successful_count = len(processed_coin_ids)
                    failed_count = len(failed_coin_ids)
                    logger.info(f"Attempted {attempted_count}/{total_coins} coins ({progress_pct:.1f}%) - {successful_count} processed, {failed_count} failed, {mapped_count} mapped")
                
                # Save checkpoint periodically (based on coins attempted)
                if self.checkpoint_enabled and attempted_count % self.checkpoint_frequency == 0:
                    logger.info(f"Saving checkpoint at coin {attempted_count}/{total_coins}")
                    self._save_checkpoint(i, total_coins, processed_coin_ids, mapping, failed_coin_ids, start_time)
                    
                    # Update cache incrementally
                    self._update_incremental_cache(mapping)
            
            # Final processing complete
            logger.info(f"Built Kraken mapping for {mapped_count} coins ({len(failed_coin_ids)} failed)")
            self.last_update = datetime.now()
            
            # Save final cache (remove partial_update flag)
            self._save_mapping_cache(mapping)
            
            # Clear checkpoint since we're done
            self._clear_checkpoint()
            
            return mapping
            
        except KeyboardInterrupt:
            logger.info("Mapping process interrupted by user")
            # Save checkpoint before exiting
            if self.checkpoint_enabled and 'i' in locals():
                logger.info("Saving checkpoint before exit...")
                self._save_checkpoint(i, total_coins, processed_coin_ids, mapping, failed_coin_ids, start_time)
                self._update_incremental_cache(mapping)
            raise
            
        except Exception as e:
            logger.error(f"Failed to build Kraken mapping: {e}")
            # Save checkpoint on error
            if self.checkpoint_enabled and 'i' in locals():
                logger.info("Saving checkpoint due to error...")
                self._save_checkpoint(i, total_coins, processed_coin_ids, mapping, failed_coin_ids, start_time)
                self._update_incremental_cache(mapping)
            return mapping if 'mapping' in locals() else {}
    
    def _extract_kraken_info(self, exchange_data: Dict) -> Optional[Dict]:
        """Extract Kraken information from CoinGecko exchange data"""
        try:
            tickers = exchange_data.get('tickers', [])
            
            for ticker in tickers:
                market = ticker.get('market', {})
                if market.get('identifier', '').lower() == 'kraken':
                    base = ticker.get('base', '')
                    target = ticker.get('target', '')
                    
                    if base and target:
                        # Find the official Kraken pair name
                        pair_name = self._find_kraken_pair_name(base, target)
                        
                        kraken_info = {
                            'exchange_name': 'kraken',
                            'symbol': f"{base}{target}",
                            'pair_name': pair_name or f"{base}{target}",
                            'base_currency': base,
                            'target_currency': target,
                            'alt_name': '',
                            'trade_url': ticker.get('trade_url', ''),
                            'is_active': True,
                            'min_order_size': 0.0,
                            'fee_percent': 0.0
                        }
                        
                        # Get additional info from Kraken pairs data
                        if pair_name and pair_name in self.asset_pairs:
                            pair_data = self.asset_pairs[pair_name]
                            kraken_info['alt_name'] = pair_data.get('altname', '')
                            kraken_info['min_order_size'] = float(pair_data.get('ordermin', 0))
                        
                        return kraken_info
            
            return None
            
        except Exception as e:
            logger.debug(f"Failed to extract Kraken info: {e}")
            return None
    
    def _find_kraken_pair_name(self, base: str, target: str) -> Optional[str]:
        """Find the official Kraken pair name for base/target currencies"""
        if not self.asset_pairs:
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
            if combo in self.asset_pairs:
                return combo
        
        # Try matching by altname
        for pair_name, pair_info in self.asset_pairs.items():
            altname = pair_info.get('altname', '')
            if altname == f"{base}{target}" or altname == f"{base.upper()}{target.upper()}":
                return pair_name
        
        return None
    
    def _save_mapping_cache(self, mapping: Dict) -> bool:
        """Save mapping to cache file"""
        try:
            cache_data = {
                'mapping': mapping,
                'last_update': self.last_update.isoformat() if self.last_update else None,
                'exchange': self.exchange_name
            }
            
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            logger.info(f"Saved Kraken mapping cache to {self.cache_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save mapping cache: {e}")
            return False
    
    def _load_mapping_cache(self) -> bool:
        """Load mapping from cache file"""
        try:
            if not os.path.exists(self.cache_file):
                return False
            
            # Check if cache is expired
            if self._is_cache_expired():
                logger.info("Mapping cache is expired")
                return False
            
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
            
            # CRITICAL FIX: Check if cache is marked as partial/incomplete
            if cache_data.get('partial_update', False):
                logger.info(f"Cache file marked as partial/incomplete - ignoring cache")
                return False
            
            # Check if there's an active checkpoint indicating incomplete mapping
            if self.checkpoint_enabled and os.path.exists(self.checkpoint_file):
                checkpoint_data = self._load_checkpoint()
                if checkpoint_data and checkpoint_data.get('status') == 'in_progress':
                    logger.info(f"Active checkpoint detected - cache is incomplete, will resume mapping")
                    return False
            
            # Check if cache has a reasonable number of entries (basic sanity check)
            mapping_data = cache_data.get('mapping', {})
            if len(mapping_data) < 10:  # Arbitrary threshold for "reasonable" cache
                logger.info(f"Cache has too few entries ({len(mapping_data)}) - likely incomplete")
                return False
            
            self.mapping_cache = mapping_data
            last_update_str = cache_data.get('last_update')
            
            if last_update_str:
                self.last_update = datetime.fromisoformat(last_update_str)
            
            logger.info(f"Loaded Kraken mapping from cache: {len(self.mapping_cache)} entries")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load mapping cache: {e}")
            return False
    
    def _is_cache_expired(self) -> bool:
        """Check if the cache file is expired"""
        try:
            if not os.path.exists(self.cache_file):
                return True
            
            file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(self.cache_file))
            return file_age.total_seconds() > (self.cache_expiry_hours * 3600)
            
        except Exception as e:
            logger.debug(f"Error checking cache expiry: {e}")
            return True
    
    def build_mapping(self, data_provider: AbstractDataProvider) -> Dict:
        """Build or load cached mapping"""
        # Try to load from cache first
        if self.config.getboolean('MAPPING', 'use_cached_mapping', True):
            if self._load_mapping_cache():
                return self.mapping_cache
        
        # Build new mapping
        return super().build_mapping(data_provider)
    
    def get_kraken_pair_info(self, pair_name: str) -> Optional[Dict]:
        """Get detailed information about a Kraken trading pair"""
        return self.asset_pairs.get(pair_name)
    
    def get_supported_quote_currencies(self) -> List[str]:
        """Get list of quote currencies supported by Kraken"""
        quote_currencies = set()
        
        for pair_data in self.asset_pairs.values():
            quote = pair_data.get('quote', '')
            if quote:
                quote_currencies.add(quote)
        
        return sorted(list(quote_currencies))
    
    def get_pairs_by_base_currency(self, base_currency: str) -> List[str]:
        """Get all trading pairs for a specific base currency"""
        pairs = []
        
        for pair_name, pair_data in self.asset_pairs.items():
            if pair_data.get('base', '').upper() == base_currency.upper():
                pairs.append(pair_name)
        
        return pairs
    
    # Checkpoint/Resume functionality methods
    
    def _save_checkpoint(self, processed_index: int, total_coins: int, processed_coin_ids: List[str], 
                        mapping_data: Dict, failed_coin_ids: List[str] = None, start_time: datetime = None) -> bool:
        """Save current mapping progress to checkpoint file"""
        if not self.checkpoint_enabled:
            return True
            
        try:
            checkpoint_data = {
                'status': 'in_progress',
                'total_coins': total_coins,
                'processed_coins': len(processed_coin_ids),
                'last_processed_index': processed_index,
                'processed_coin_ids': processed_coin_ids,
                'failed_coin_ids': failed_coin_ids or [],
                'retry_counts': self.retry_counts,
                'start_time': start_time.isoformat() if start_time else datetime.now().isoformat(),
                'last_checkpoint_time': datetime.now().isoformat(),
                'batch_size': 50,  # Current batch size from _build_coin_mapping
                'checkpoint_frequency': self.checkpoint_frequency,
                'mapping_file': self.cache_file,
                'partial_mapping_count': len(mapping_data)
            }
            
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            
            logger.debug(f"Checkpoint saved: {processed_index + 1}/{total_coins} coins processed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
            return False
    
    def _load_checkpoint(self) -> Optional[Dict]:
        """Load checkpoint data from file"""
        if not self.checkpoint_enabled or not os.path.exists(self.checkpoint_file):
            return None
            
        try:
            with open(self.checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
            
            if self._validate_checkpoint(checkpoint_data):
                logger.info(f"Loaded checkpoint: {checkpoint_data['processed_coins']}/{checkpoint_data['total_coins']} coins processed")
                return checkpoint_data
            else:
                logger.warning("Checkpoint validation failed, starting fresh")
                return None
                
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return None
    
    def _clear_checkpoint(self) -> bool:
        """Remove checkpoint file when mapping is complete"""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
                logger.info("Checkpoint file cleared")
            return True
        except Exception as e:
            logger.error(f"Failed to clear checkpoint: {e}")
            return False
    
    def _validate_checkpoint(self, checkpoint_data: Dict) -> bool:
        """Validate checkpoint data structure and integrity"""
        try:
            required_fields = [
                'status', 'total_coins', 'processed_coins', 'last_processed_index',
                'processed_coin_ids', 'start_time', 'last_checkpoint_time'
            ]
            
            # Check required fields exist
            for field in required_fields:
                if field not in checkpoint_data:
                    logger.warning(f"Checkpoint missing required field: {field}")
                    return False
            
            # Validate data types and ranges
            if not isinstance(checkpoint_data['processed_coin_ids'], list):
                logger.warning("Checkpoint processed_coin_ids is not a list")
                return False
            
            if checkpoint_data['processed_coins'] != len(checkpoint_data['processed_coin_ids']):
                logger.warning("Checkpoint processed_coins count mismatch")
                return False
            
            if checkpoint_data['last_processed_index'] < 0:
                logger.warning("Checkpoint has invalid processed index")
                return False
            
            # Check if checkpoint is not too old (24 hours)
            try:
                checkpoint_time = datetime.fromisoformat(checkpoint_data['last_checkpoint_time'])
                age_hours = (datetime.now() - checkpoint_time).total_seconds() / 3600
                if age_hours > 24:
                    logger.warning(f"Checkpoint is too old ({age_hours:.1f} hours)")
                    return False
            except ValueError:
                logger.warning("Checkpoint has invalid timestamp")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating checkpoint: {e}")
            return False
    
    def _should_resume(self) -> bool:
        """Determine if we should resume from checkpoint"""
        if not self.checkpoint_enabled or not self.resume_on_restart:
            return False
        
        checkpoint_data = self._load_checkpoint()
        return checkpoint_data is not None and checkpoint_data.get('status') == 'in_progress'
    
    def _get_resume_point(self, all_coins: List[Dict]) -> Tuple[int, List[str], Dict]:
        """Get resume point information from checkpoint
        
        Returns:
            Tuple of (resume_index, processed_coin_ids, existing_mapping)
        """
        checkpoint_data = self._load_checkpoint()
        if not checkpoint_data:
            return 0, [], {}
        
        resume_index = checkpoint_data.get('last_processed_index', 0) + 1
        processed_coin_ids = checkpoint_data.get('processed_coin_ids', [])
        
        # Load existing partial mapping from cache if it exists
        existing_mapping = {}
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    cache_data = json.load(f)
                    existing_mapping = cache_data.get('mapping', {})
            except Exception as e:
                logger.debug(f"Could not load partial mapping: {e}")
        
        logger.info(f"Resuming from index {resume_index}, {len(processed_coin_ids)} coins already processed")
        return resume_index, processed_coin_ids, existing_mapping
    
    def _update_incremental_cache(self, mapping_data: Dict) -> bool:
        """Update the main cache file incrementally during mapping process"""
        try:
            cache_data = {
                'mapping': mapping_data,
                'last_update': datetime.now().isoformat(),
                'exchange': self.exchange_name,
                'partial_update': True  # Flag to indicate this is a partial update
            }
            
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update incremental cache: {e}")
            return False
    
    def _get_retry_count(self, coin_id: str, checkpoint_data: Optional[Dict] = None) -> int:
        """Get the current retry count for a coin"""
        # First check in-memory retry counts
        if coin_id in self.retry_counts:
            return self.retry_counts[coin_id]
        
        # Check checkpoint data for retry counts
        if checkpoint_data and 'retry_counts' in checkpoint_data:
            return checkpoint_data['retry_counts'].get(coin_id, 0)
        
        return 0
    
    def _update_retry_count(self, coin_id: str, count: int) -> None:
        """Update the retry count for a coin"""
        self.retry_counts[coin_id] = count