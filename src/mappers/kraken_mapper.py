"""
Kraken Exchange Mapper with Checkpoint/Resume Support
Maps CoinGecko coin data to Kraken exchange symbols with progress tracking
"""

import json
import time
import logging
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, asdict
import requests
import pandas as pd

from .abstract_exchange_mapper import AbstractExchangeMapper


@dataclass
class CheckpointData:
    """Data structure for checkpoint information"""
    version: str = "1.0"
    timestamp: str = ""
    processed_coins: List[str] = None
    failed_coins: List[str] = None
    mappings: Dict[str, Dict] = None
    exchange_data: Dict = None
    current_batch: int = 0
    total_batches: int = 0
    last_api_call: str = ""
    api_call_count: int = 0
    
    def __post_init__(self):
        if self.processed_coins is None:
            self.processed_coins = []
        if self.failed_coins is None:
            self.failed_coins = []
        if self.mappings is None:
            self.mappings = {}
        if self.exchange_data is None:
            self.exchange_data = {}


class KrakenMapper(AbstractExchangeMapper):
    """
    Kraken Exchange Mapper with checkpoint/resume support
    Maps CoinGecko coin data to Kraken exchange symbols with progress persistence
    """
    
    def __init__(self, config_manager=None):
        super().__init__(config_manager)
        self.exchange_name = "kraken"
        self.api_base_url = "https://api.kraken.com/0/public"
        self.coingecko_base_url = "https://api.coingecko.com/api/v3"
        
        # Checkpoint configuration
        self.checkpoint_dir = Path("checkpoints")
        self.checkpoint_file = self.checkpoint_dir / f"kraken_mapper_checkpoint.json"
        self.checkpoint_interval = 50  # Save every 50 processed coins
        self.max_checkpoint_age_hours = 24  # Expire checkpoints after 24 hours
        
        # Rate limiting
        self.api_delay = 1.0  # Delay between API calls in seconds
        self.max_retries = 3
        self.retry_delay = 5.0
        
        # Progress tracking
        self.checkpoint_data = CheckpointData()
        self.logger = logging.getLogger(__name__)
        
        # Create checkpoint directory
        self.checkpoint_dir.mkdir(exist_ok=True)
        
        # Initialize mapping data
        self._initialize_static_mappings()
    
    def _initialize_static_mappings(self):
        """Initialize static symbol mappings that don't require API calls"""
        self.static_mappings = {
            # Major cryptocurrencies with known mappings
            'bitcoin': {'kraken_symbol': 'XXBT', 'display_name': 'Bitcoin', 'base_currency': 'XBT'},
            'ethereum': {'kraken_symbol': 'XETH', 'display_name': 'Ethereum', 'base_currency': 'ETH'},
            'litecoin': {'kraken_symbol': 'XLTC', 'display_name': 'Litecoin', 'base_currency': 'LTC'},
            'bitcoin-cash': {'kraken_symbol': 'BCH', 'display_name': 'Bitcoin Cash', 'base_currency': 'BCH'},
            'ripple': {'kraken_symbol': 'XXRP', 'display_name': 'XRP', 'base_currency': 'XRP'},
            'cardano': {'kraken_symbol': 'ADA', 'display_name': 'Cardano', 'base_currency': 'ADA'},
            'chainlink': {'kraken_symbol': 'LINK', 'display_name': 'Chainlink', 'base_currency': 'LINK'},
            'stellar': {'kraken_symbol': 'XXLM', 'display_name': 'Stellar', 'base_currency': 'XLM'},
            'monero': {'kraken_symbol': 'XXMR', 'display_name': 'Monero', 'base_currency': 'XMR'},
            'zcash': {'kraken_symbol': 'ZEC', 'display_name': 'Zcash', 'base_currency': 'ZEC'},
            'tezos': {'kraken_symbol': 'XTZ', 'display_name': 'Tezos', 'base_currency': 'XTZ'},
            'compound': {'kraken_symbol': 'COMP', 'display_name': 'Compound', 'base_currency': 'COMP'},
            'uniswap': {'kraken_symbol': 'UNI', 'display_name': 'Uniswap', 'base_currency': 'UNI'},
            'aave': {'kraken_symbol': 'AAVE', 'display_name': 'Aave', 'base_currency': 'AAVE'},
            'polygon': {'kraken_symbol': 'MATIC', 'display_name': 'Polygon', 'base_currency': 'MATIC'},
            'solana': {'kraken_symbol': 'SOL', 'display_name': 'Solana', 'base_currency': 'SOL'},
            'polkadot': {'kraken_symbol': 'DOT', 'display_name': 'Polkadot', 'base_currency': 'DOT'},
            'algorand': {'kraken_symbol': 'ALGO', 'display_name': 'Algorand', 'base_currency': 'ALGO'},
            'cosmos': {'kraken_symbol': 'ATOM', 'display_name': 'Cosmos', 'base_currency': 'ATOM'},
            'avalanche-2': {'kraken_symbol': 'AVAX', 'display_name': 'Avalanche', 'base_currency': 'AVAX'}
        }
    
    def check_for_existing_checkpoint(self) -> bool:
        """Check if a valid checkpoint exists and can be resumed"""
        if not self.checkpoint_file.exists():
            self.logger.info("No existing checkpoint found")
            return False
        
        try:
            with open(self.checkpoint_file, 'r') as f:
                checkpoint_dict = json.load(f)
            
            # Check checkpoint age
            checkpoint_time = datetime.fromisoformat(checkpoint_dict.get('timestamp', ''))
            age_hours = (datetime.now() - checkpoint_time).total_seconds() / 3600
            
            if age_hours > self.max_checkpoint_age_hours:
                self.logger.warning(f"Checkpoint is {age_hours:.1f} hours old, too old to resume")
                self._cleanup_checkpoint()
                return False
            
            # Load checkpoint data
            self.checkpoint_data = CheckpointData(**checkpoint_dict)
            self.logger.info(f"Found valid checkpoint from {checkpoint_time}")
            self.logger.info(f"Progress: {len(self.checkpoint_data.processed_coins)} coins processed, "
                           f"{len(self.checkpoint_data.failed_coins)} failed")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load checkpoint: {e}")
            self._cleanup_checkpoint()
            return False
    
    def save_checkpoint(self, force: bool = False):
        """Save current progress to checkpoint file"""
        try:
            # Only save if we've processed enough coins or forced
            if not force and len(self.checkpoint_data.processed_coins) % self.checkpoint_interval != 0:
                return
            
            self.checkpoint_data.timestamp = datetime.now().isoformat()
            
            with open(self.checkpoint_file, 'w') as f:
                json.dump(asdict(self.checkpoint_data), f, indent=2)
            
            self.logger.debug(f"Checkpoint saved: {len(self.checkpoint_data.processed_coins)} coins processed")
            
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")
    
    def _cleanup_checkpoint(self):
        """Remove checkpoint file"""
        try:
            if self.checkpoint_file.exists():
                self.checkpoint_file.unlink()
                self.logger.info("Checkpoint file cleaned up")
        except Exception as e:
            self.logger.error(f"Failed to cleanup checkpoint: {e}")
    
    def _make_api_call(self, url: str, max_retries: int = None) -> Optional[Dict]:
        """Make API call with rate limiting and retry logic"""
        if max_retries is None:
            max_retries = self.max_retries
        
        for attempt in range(max_retries + 1):
            try:
                # Rate limiting
                time.sleep(self.api_delay)
                
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                # Track API calls
                self.checkpoint_data.api_call_count += 1
                self.checkpoint_data.last_api_call = datetime.now().isoformat()
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    self.logger.warning(f"API call failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    self.logger.error(f"API call failed after {max_retries + 1} attempts: {e}")
                    return None
    
    def _get_kraken_assets(self) -> Dict:
        """Get all available assets from Kraken API with caching"""
        # Check if we have cached exchange data
        if self.checkpoint_data.exchange_data:
            self.logger.info("Using cached Kraken assets from checkpoint")
            return self.checkpoint_data.exchange_data
        
        self.logger.info("Fetching Kraken assets...")
        
        url = f"{self.api_base_url}/Assets"
        data = self._make_api_call(url)
        
        if data and 'result' in data:
            self.checkpoint_data.exchange_data = data['result']
            self.save_checkpoint()  # Save after getting exchange data
            self.logger.info(f"Retrieved {len(data['result'])} Kraken assets")
            return data['result']
        else:
            self.logger.error("Failed to fetch Kraken assets")
            return {}
    
    def _get_kraken_ticker_pairs(self) -> Dict:
        """Get all trading pairs from Kraken API"""
        self.logger.info("Fetching Kraken trading pairs...")
        
        url = f"{self.api_base_url}/AssetPairs"
        data = self._make_api_call(url)
        
        if data and 'result' in data:
            self.logger.info(f"Retrieved {len(data['result'])} Kraken trading pairs")
            return data['result']
        else:
            self.logger.error("Failed to fetch Kraken trading pairs")
            return {}
    
    def _get_coingecko_exchanges_data(self, coin_id: str) -> Optional[Dict]:
        """Get exchange data for a specific coin from CoinGecko"""
        url = f"{self.coingecko_base_url}/coins/{coin_id}/tickers"
        
        data = self._make_api_call(url)
        
        if data and 'tickers' in data:
            # Filter for Kraken exchange
            kraken_tickers = [ticker for ticker in data['tickers'] 
                            if ticker.get('market', {}).get('identifier') == 'kraken']
            return kraken_tickers
        
        return None
    
    def _create_mapping_for_coin(self, coin_id: str, coin_data: Dict) -> Optional[Dict]:
        """Create mapping for a single coin"""
        try:
            # Check if coin is already processed
            if coin_id in self.checkpoint_data.processed_coins:
                return self.checkpoint_data.mappings.get(coin_id)
            
            # Check if coin previously failed
            if coin_id in self.checkpoint_data.failed_coins:
                self.logger.debug(f"Skipping previously failed coin: {coin_id}")
                return None
            
            # Check static mappings first
            if coin_id in self.static_mappings:
                mapping = self.static_mappings[coin_id].copy()
                mapping.update({
                    'coingecko_id': coin_id,
                    'mapping_source': 'static',
                    'confidence': 'high'
                })
                
                self.checkpoint_data.mappings[coin_id] = mapping
                self.checkpoint_data.processed_coins.append(coin_id)
                
                self.logger.debug(f"Static mapping found for {coin_id}: {mapping['kraken_symbol']}")
                return mapping
            
            # Try to get exchange data from CoinGecko
            exchange_data = self._get_coingecko_exchanges_data(coin_id)
            
            if exchange_data:
                # Process Kraken ticker data
                for ticker in exchange_data:
                    base = ticker.get('base', '').upper()
                    target = ticker.get('target', '').upper()
                    
                    if base and target:
                        mapping = {
                            'coingecko_id': coin_id,
                            'kraken_symbol': base,
                            'display_name': coin_data.get('name', coin_id),
                            'base_currency': base,
                            'target_currency': target,
                            'mapping_source': 'api',
                            'confidence': 'medium',
                            'last_updated': datetime.now().isoformat()
                        }
                        
                        self.checkpoint_data.mappings[coin_id] = mapping
                        self.checkpoint_data.processed_coins.append(coin_id)
                        
                        self.logger.debug(f"API mapping created for {coin_id}: {base}")
                        return mapping
            
            # If no mapping found, try symbol matching
            symbol = coin_data.get('symbol', '').upper()
            if symbol:
                kraken_assets = self._get_kraken_assets()
                
                # Try direct symbol match
                for asset_key, asset_data in kraken_assets.items():
                    if asset_data.get('altname', '').upper() == symbol:
                        mapping = {
                            'coingecko_id': coin_id,
                            'kraken_symbol': asset_key,
                            'display_name': coin_data.get('name', coin_id),
                            'base_currency': asset_data.get('altname', symbol),
                            'mapping_source': 'symbol_match',
                            'confidence': 'low',
                            'last_updated': datetime.now().isoformat()
                        }
                        
                        self.checkpoint_data.mappings[coin_id] = mapping
                        self.checkpoint_data.processed_coins.append(coin_id)
                        
                        self.logger.debug(f"Symbol mapping found for {coin_id}: {asset_key}")
                        return mapping
            
            # No mapping found
            self.checkpoint_data.failed_coins.append(coin_id)
            self.logger.debug(f"No mapping found for {coin_id}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error creating mapping for {coin_id}: {e}")
            self.checkpoint_data.failed_coins.append(coin_id)
            return None
    
    def build_coin_mapping(self, coin_data: List[Dict]) -> Dict:
        """
        Build mapping between CoinGecko IDs and Kraken data with checkpoint support
        
        Args:
            coin_data: List of coin dictionaries from CoinGecko
            
        Returns:
            Dictionary mapping CoinGecko IDs to Kraken symbol data
        """
        start_time = time.time()
        
        # Check for existing checkpoint
        resumed = self.check_for_existing_checkpoint()
        
        if resumed:
            self.logger.info("Resuming from checkpoint...")
        else:
            self.logger.info("Starting fresh mapping process...")
            self.checkpoint_data = CheckpointData()
        
        # Setup batch processing
        total_coins = len(coin_data)
        batch_size = 100
        self.checkpoint_data.total_batches = (total_coins + batch_size - 1) // batch_size
        
        processed_count = len(self.checkpoint_data.processed_coins)
        failed_count = len(self.checkpoint_data.failed_coins)
        
        self.logger.info(f"Processing {total_coins} coins (already processed: {processed_count}, failed: {failed_count})")
        
        # Create lookup for already processed coins
        processed_set = set(self.checkpoint_data.processed_coins + self.checkpoint_data.failed_coins)
        
        try:
            # Pre-fetch Kraken assets once
            self._get_kraken_assets()
            
            # Process coins in batches
            for batch_start in range(0, total_coins, batch_size):
                batch_end = min(batch_start + batch_size, total_coins)
                current_batch = batch_start // batch_size + 1
                
                self.checkpoint_data.current_batch = current_batch
                
                self.logger.info(f"Processing batch {current_batch}/{self.checkpoint_data.total_batches} "
                               f"(coins {batch_start + 1}-{batch_end})")
                
                batch_processed = 0
                batch_mapped = 0
                
                for i in range(batch_start, batch_end):
                    coin = coin_data[i]
                    coin_id = coin.get('id')
                    
                    if not coin_id:
                        continue
                    
                    # Skip if already processed
                    if coin_id in processed_set:
                        continue
                    
                    # Create mapping
                    mapping = self._create_mapping_for_coin(coin_id, coin)
                    
                    if mapping:
                        batch_mapped += 1
                    
                    batch_processed += 1
                    
                    # Save checkpoint periodically
                    if (processed_count + batch_processed) % self.checkpoint_interval == 0:
                        self.save_checkpoint()
                        self.logger.info(f"Progress: {processed_count + batch_processed}/{total_coins} coins processed")
                
                processed_count += batch_processed
                
                self.logger.info(f"Batch {current_batch} complete: {batch_mapped} mapped, {batch_processed} processed")
        
        except KeyboardInterrupt:
            self.logger.warning("Process interrupted by user")
            self.save_checkpoint(force=True)
            raise
        
        except Exception as e:
            self.logger.error(f"Error during mapping process: {e}")
            self.save_checkpoint(force=True)
            raise
        
        # Final save and cleanup
        self.save_checkpoint(force=True)
        
        execution_time = time.time() - start_time
        successful_mappings = len(self.checkpoint_data.mappings)
        failed_mappings = len(self.checkpoint_data.failed_coins)
        
        self.logger.info(f"Mapping complete!")
        self.logger.info(f"  Total processed: {processed_count}")
        self.logger.info(f"  Successful mappings: {successful_mappings}")
        self.logger.info(f"  Failed mappings: {failed_mappings}")
        self.logger.info(f"  API calls made: {self.checkpoint_data.api_call_count}")
        self.logger.info(f"  Execution time: {execution_time:.2f} seconds")
        
        # Cleanup checkpoint on successful completion
        self._cleanup_checkpoint()
        
        return self.checkpoint_data.mappings
    
    def map_coins_to_exchange(self, coin_data: List[Dict]) -> List[Dict]:
        """
        Map coins to Kraken exchange format
        
        Args:
            coin_data: List of coin dictionaries from CoinGecko
            
        Returns:
            List of mapped coin dictionaries with Kraken data
        """
        # Build the mapping
        mappings = self.build_coin_mapping(coin_data)
        
        # Apply mappings to coin data
        mapped_coins = []
        
        for coin in coin_data:
            coin_id = coin.get('id')
            if coin_id in mappings:
                mapped_coin = coin.copy()
                mapped_coin.update({
                    'kraken_data': mappings[coin_id],
                    'is_tradeable_on_kraken': True
                })
                mapped_coins.append(mapped_coin)
            else:
                # Add coin without Kraken data
                mapped_coin = coin.copy()
                mapped_coin.update({
                    'kraken_data': None,
                    'is_tradeable_on_kraken': False
                })
                mapped_coins.append(mapped_coin)
        
        self.logger.info(f"Mapped {len(mapped_coins)} coins, "
                        f"{len(mappings)} tradeable on Kraken")
        
        return mapped_coins
    
    def get_exchange_info(self) -> Dict:
        """Get exchange information"""
        return {
            'name': 'Kraken',
            'identifier': 'kraken',
            'api_base_url': self.api_base_url,
            'supported_features': [
                'spot_trading',
                'margin_trading',
                'futures_trading',
                'api_access'
            ],
            'fee_structure': 'maker_taker',
            'checkpoint_support': True,
            'last_mapping_update': self.checkpoint_data.last_api_call
        }
    
    def validate_mappings(self, mappings: Dict) -> Dict:
        """Validate existing mappings against current Kraken data"""
        validation_results = {
            'valid': 0,
            'invalid': 0,
            'outdated': 0,
            'errors': []
        }
        
        try:
            kraken_assets = self._get_kraken_assets()
            current_symbols = set(asset_data.get('altname', '') for asset_data in kraken_assets.values())
            
            for coin_id, mapping in mappings.items():
                try:
                    kraken_symbol = mapping.get('base_currency', '')
                    
                    if kraken_symbol in current_symbols:
                        validation_results['valid'] += 1
                    else:
                        validation_results['invalid'] += 1
                        validation_results['errors'].append(f"{coin_id}: Symbol {kraken_symbol} not found on Kraken")
                
                except Exception as e:
                    validation_results['errors'].append(f"{coin_id}: Validation error - {e}")
        
        except Exception as e:
            validation_results['errors'].append(f"Failed to fetch Kraken data for validation: {e}")
        
        return validation_results
    
    def get_checkpoint_status(self) -> Dict:
        """Get current checkpoint status"""
        return {
            'checkpoint_exists': self.checkpoint_file.exists(),
            'checkpoint_file': str(self.checkpoint_file),
            'processed_coins': len(self.checkpoint_data.processed_coins),
            'failed_coins': len(self.checkpoint_data.failed_coins),
            'successful_mappings': len(self.checkpoint_data.mappings),
            'current_batch': self.checkpoint_data.current_batch,
            'total_batches': self.checkpoint_data.total_batches,
            'api_calls_made': self.checkpoint_data.api_call_count,
            'last_checkpoint': self.checkpoint_data.timestamp,
            'last_api_call': self.checkpoint_data.last_api_call
        }
