"""
Kraken Exchange Mapper Implementation
Handles mapping between CoinGecko coins and Kraken trading pairs
"""

import json
import os
import requests
import time
from datetime import datetime
from typing import Dict, List, Optional
import logging

from .abstract_exchange_mapper import AbstractExchangeMapper, ExchangeInfo

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
            response = self.session.get(f"{self.kraken_api_url}/Assets", timeout=30)
            response.raise_for_status()
            
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
            response = self.session.get(f"{self.kraken_api_url}/AssetPairs", timeout=30)
            response.raise_for_status()
            
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
    
    def _build_coin_mapping(self) -> Dict:
        """Build mapping between CoinGecko IDs and Kraken data using CoinGecko API"""
        mapping = {}
        
        try:
            # This would typically use the CoinGecko provider to get exchange data
            # For now, we'll build a basic mapping using known conversions
            from coingecko_provider import CoinGeckoProvider
            
            coingecko = CoinGeckoProvider(self.config)
            
            # Get all coins from CoinGecko
            all_coins = coingecko.get_all_coins()
            
            mapped_count = 0
            batch_size = 50
            
            for i in range(0, len(all_coins), batch_size):
                batch = all_coins[i:i + batch_size]
                
                for coin in batch:
                    coin_id = coin['id']
                    
                    try:
                        # Get exchange data for this coin
                        exchange_data = coingecko.get_exchange_data(coin_id)
                        
                        if exchange_data:
                            kraken_info = self._extract_kraken_info(exchange_data)
                            if kraken_info:
                                mapping[coin_id] = kraken_info
                                mapped_count += 1
                        
                        # Rate limiting
                        time.sleep(self.config.getfloat('IMPORT', 'rate_limit_delay', 1.5))
                        
                    except Exception as e:
                        logger.debug(f"Failed to process {coin_id}: {e}")
                        continue
                
                logger.info(f"Processed {min(i + batch_size, len(all_coins))}/{len(all_coins)} coins")
            
            logger.info(f"Built Kraken mapping for {mapped_count} coins")
            self.last_update = datetime.now()
            
            # Save to cache
            self._save_mapping_cache(mapping)
            
            return mapping
            
        except Exception as e:
            logger.error(f"Failed to build Kraken mapping: {e}")
            return {}
    
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
            
            self.mapping_cache = cache_data.get('mapping', {})
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
    
    def build_mapping(self) -> Dict:
        """Build or load cached mapping"""
        # Try to load from cache first
        if self.config.getboolean('MAPPING', 'use_cached_mapping', True):
            if self._load_mapping_cache():
                return self.mapping_cache
        
        # Build new mapping
        return super().build_mapping()
    
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
