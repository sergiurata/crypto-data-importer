"""
AmiBroker Database Adapter Implementation
Handles all AmiBroker-specific database operations
"""

import os
import win32com.client
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

from .abstract_database_adapter import AbstractDatabaseAdapter

logger = logging.getLogger(__name__)


class AmiBrokerAdapter(AbstractDatabaseAdapter):
    """AmiBroker-specific implementation of AbstractDatabaseAdapter"""
    
    def __init__(self, config):
        super().__init__(config)
        self.com_object = None
        self.current_database = None
        
    def connect(self, database_path: str) -> bool:
        """Connect to AmiBroker and load specified database"""
        try:
            # Initialize COM object
            self.com_object = win32com.client.Dispatch("Broker.Application")
            logger.info("AmiBroker COM connection established")
            
            self.database_path = database_path
            
            # Load database if path provided
            if database_path:
                return self._load_database(database_path)
            else:
                # Use current database
                current_db = self._get_current_database()
                if current_db:
                    self.database_path = current_db
                    self.connection_verified = True
                    logger.info(f"Using current AmiBroker database: {current_db}")
                    return True
                else:
                    logger.warning("No current database found in AmiBroker")
                    return False
            
        except Exception as e:
            logger.error(f"Failed to connect to AmiBroker: {e}")
            return False
    
    def _load_database(self, database_path: str) -> bool:
        """Load specified AmiBroker database"""
        try:
            if not os.path.exists(database_path):
                logger.error(f"Database path does not exist: {database_path}")
                return False
            
            # Get current database for comparison
            current_db = self._get_current_database()
            
            # Normalize paths for comparison
            current_db_normalized = os.path.normpath(current_db).lower() if current_db else ""
            target_db_normalized = os.path.normpath(database_path).lower()
            
            if current_db_normalized == target_db_normalized:
                logger.info(f"Database already loaded: {database_path}")
                self.connection_verified = True
                return True
            
            logger.info(f"Loading AmiBroker database: {database_path}")
            
            # Load the database
            result = self.com_object.LoadDatabase(database_path)
            
            if result:
                logger.info(f"Successfully loaded database: {database_path}")
                self.connection_verified = True
                self.current_database = database_path
                return True
            else:
                logger.error(f"Failed to load database: {database_path}")
                return False
                
        except Exception as e:
            logger.error(f"Error loading database: {e}")
            return False
    
    def _get_current_database(self) -> Optional[str]:
        """Get the path of currently loaded AmiBroker database"""
        try:
            # Try DatabasePath property
            try:
                db_path = self.com_object.DatabasePath
                if db_path:
                    return db_path
            except AttributeError:
                pass
            
            # Try through Documents collection
            try:
                if hasattr(self.com_object, 'Documents') and self.com_object.Documents.Count > 0:
                    doc = self.com_object.Documents(0)
                    if hasattr(doc, 'Path'):
                        return doc.Path
            except (AttributeError, IndexError):
                pass
            
            # Try ActiveDocument
            try:
                if hasattr(self.com_object, 'ActiveDocument') and self.com_object.ActiveDocument:
                    active_doc = self.com_object.ActiveDocument
                    if hasattr(active_doc, 'Path'):
                        return active_doc.Path
            except AttributeError:
                pass
            
            return None
            
        except Exception as e:
            logger.debug(f"Error getting current database: {e}")
            return None
    
    def create_database(self, path: str) -> bool:
        """Create a new AmiBroker database"""
        try:
            # Check if database already exists
            if os.path.exists(path):
                logger.error(f"Database already exists: {path}")
                return False
            
            # Ensure directory exists
            db_dir = os.path.dirname(path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
                logger.info(f"Created directory: {db_dir}")
            
            # Create new database
            logger.info(f"Creating new AmiBroker database: {path}")
            
            if not self.com_object:
                if not self.connect(""):
                    return False
            
            result = self.com_object.NewDatabase(path)
            
            if result:
                logger.info(f"Successfully created database: {path}")
                self.database_path = path
                self.connection_verified = True
                return True
            else:
                logger.error(f"Failed to create database: {path}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating database: {e}")
            return False
    
    def import_data(self, symbol: str, data: pd.DataFrame, metadata: Dict = None) -> bool:
        """Import new data for a symbol"""
        try:
            if not self.validate_data_format(data):
                return False
            
            # Get or create stock object
            stock = self.com_object.Stocks(symbol)
            
            # Set stock properties
            if metadata:
                self._set_stock_metadata(stock, metadata)
            
            # Import quotations
            quotations = stock.Quotations
            
            for date, row in data.iterrows():
                # Type hint for PyCharm: date is pandas.Timestamp
                dt = pd.Timestamp(date).to_pydatetime()
                quote = quotations.Add(dt)
                quote.Open = float(row['Open'])
                quote.High = float(row['High'])
                quote.Low = float(row['Low'])
                quote.Close = float(row['Close'])
                quote.Volume = float(row['Volume'])
                
                # Add market cap if available
                if 'MarketCap' in row:
                    try:
                        quote.SetExtraData('MarketCap', float(row['MarketCap']))
                    except AttributeError:
                        pass
            
            # Save the stock
            stock.Save()
            logger.info(f"Imported {len(data)} records for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to import data for {symbol}: {e}")
            return False
    
    def update_data(self, symbol: str, data: pd.DataFrame) -> Tuple[int, int]:
        """Update existing data for a symbol"""
        try:
            if not self.validate_data_format(data):
                return 0, 0
            
            # Get existing data range
            existing_start, existing_end = self.get_existing_range(symbol)
            
            new_records = 0
            updated_records = 0
            
            stock = self.com_object.Stocks(symbol)
            quotations = stock.Quotations
            
            if existing_start is None:
                # No existing data, import all
                for date, row in data.iterrows():
                    self._add_quotation(quotations, date, row)
                    new_records += 1
            else:
                # Update existing and add new
                for date, row in data.iterrows():
                    dt = date.to_pydatetime()
                    
                    if dt.date() < existing_start.date() or dt.date() > existing_end.date():
                        # New data outside existing range
                        self._add_quotation(quotations, date, row)
                        new_records += 1
                    else:
                        # Potentially update existing data
                        if self._update_existing_quotation(quotations, date, row):
                            updated_records += 1
            
            # Update metadata
            try:
                stock.SetExtraData('LastUpdated', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            except AttributeError:
                pass
            
            stock.Save()
            
            return new_records, updated_records
            
        except Exception as e:
            logger.error(f"Failed to update data for {symbol}: {e}")
            return 0, 0
    
    def _add_quotation(self, quotations, date, row):
        """Add a new quotation"""
        dt = date.to_pydatetime()
        quote = quotations.Add(dt)
        quote.Open = float(row['Open'])
        quote.High = float(row['High'])
        quote.Low = float(row['Low'])
        quote.Close = float(row['Close'])
        quote.Volume = float(row['Volume'])
        
        if 'MarketCap' in row:
            try:
                quote.SetExtraData('MarketCap', float(row['MarketCap']))
            except AttributeError:
                pass
    
    def _update_existing_quotation(self, quotations, date, row) -> bool:
        """Update an existing quotation if values have changed"""
        dt = date.to_pydatetime()
        
        # Find existing quotation for this date
        for i in range(quotations.Count):
            quote = quotations(i)
            quote_date = datetime(quote.Date.year, quote.Date.month, quote.Date.day)
            
            if quote_date == dt.date():
                # Check if values have changed significantly
                if (abs(quote.Close - float(row['Close'])) > 0.0001 or
                    abs(quote.Volume - float(row['Volume'])) > 0.1):
                    
                    quote.Open = float(row['Open'])
                    quote.High = float(row['High'])
                    quote.Low = float(row['Low'])
                    quote.Close = float(row['Close'])
                    quote.Volume = float(row['Volume'])
                    
                    if 'MarketCap' in row:
                        try:
                            quote.SetExtraData('MarketCap', float(row['MarketCap']))
                        except AttributeError:
                            pass
                    
                    return True
                break
        
        return False
    
    def get_existing_range(self, symbol: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Get the date range of existing data for a symbol"""
        try:
            stock = self.com_object.Stocks(symbol)
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
            logger.debug(f"Could not get existing data range for {symbol}: {e}")
            return None, None
    
    def create_groups(self) -> bool:
        """Create organizational groups in AmiBroker (optional feature)"""
        try:
            # Check if COM object exists and is connected
            if not self.com_object:
                logger.info("AmiBroker COM object not initialized - skipping group creation")
                return True  # Return True as groups are optional
            
            # Ensure database is loaded first
            if not self.connection_verified:
                logger.info("Database not verified - skipping group creation")
                return True  # Return True as groups are optional
            
            logger.debug("Attempting to access Groups property...")
            
            # Try to access Groups property with better error handling
            try:
                groups = self.com_object.Groups
                if groups is None:
                    logger.info("AmiBroker Groups property is None - feature not available")
                    return True  # Return True as this is not critical
                    
                logger.debug(f"Successfully accessed Groups property: {groups}")
            except AttributeError as e:
                logger.info(f"AmiBroker Groups not supported in this version - skipping group creation")
                logger.debug(f"AttributeError details: {e}")
                return True  # Return True as groups are optional
            except Exception as e:
                logger.info(f"Cannot access Groups property - skipping group creation: {e}")
                return True  # Return True as groups are optional
            
            # Try to create groups (best effort)
            groups_created = 0
            
            # Group 253 for Kraken tradeable
            try:
                kraken_group = groups(253)
                kraken_group.Name = "Crypto - Kraken Tradeable"
                groups_created += 1
                logger.debug("Created Kraken group (253)")
            except Exception as e:
                logger.debug(f"Could not create Kraken group: {e}")
            
            # Group 254 for non-Kraken
            try:
                non_kraken_group = groups(254)
                non_kraken_group.Name = "Crypto - Other Exchanges"
                groups_created += 1
                logger.debug("Created non-Kraken group (254)")
            except Exception as e:
                logger.debug(f"Could not create non-Kraken group: {e}")
            
            if groups_created > 0:
                logger.info(f"Successfully created {groups_created} AmiBroker groups")
            else:
                logger.info("No AmiBroker groups created - groups may not be supported")
            
            return True  # Always return True as groups are optional
            
        except Exception as e:
            logger.info(f"Group creation skipped due to error: {e}")
            logger.debug(f"COM object state: {self.com_object}")
            return True  # Return True as groups are optional functionality
    
    def _set_stock_metadata(self, stock, metadata: Dict):
        """Set metadata for a stock object"""
        try:
            # Set basic properties
            if 'full_name' in metadata:
                stock.FullName = metadata['full_name']
            
            if 'group_id' in metadata:
                stock.GroupID = metadata['group_id']
            
            if 'market_id' in metadata:
                stock.MarketID = metadata['market_id']
            
            # Set extra data
            for key, value in metadata.items():
                if key not in ['full_name', 'group_id', 'market_id']:
                    try:
                        stock.SetExtraData(key, str(value))
                    except AttributeError:
                        pass
                        
        except Exception as e:
            logger.debug(f"Error setting stock metadata: {e}")
    
    def get_symbol_list(self) -> List[str]:
        """Get list of all symbols in the database"""
        try:
            stocks = self.com_object.Stocks
            symbols = []
            
            for i in range(stocks.Count):
                stock = stocks(i)
                if stock.Ticker:
                    symbols.append(stock.Ticker)
            
            return symbols
            
        except Exception as e:
            logger.error(f"Failed to get symbol list: {e}")
            return []
    
    def delete_symbol(self, symbol: str) -> bool:
        """Delete a symbol and all its data from the database"""
        try:
            stock = self.com_object.Stocks(symbol)
            quotations = stock.Quotations
            quotations.Clear()
            stock.Save()
            
            logger.info(f"Deleted symbol: {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete symbol {symbol}: {e}")
            return False
    
    def get_symbol_metadata(self, symbol: str) -> Optional[Dict]:
        """Get metadata for a symbol"""
        try:
            stock = self.com_object.Stocks(symbol)
            
            metadata = {
                'symbol': symbol,
                'full_name': stock.FullName,
                'group_id': stock.GroupID,
                'market_id': stock.MarketID
            }
            
            # Try to get extra data
            extra_fields = ['CoinGeckoID', 'Kraken', 'KrakenSymbol', 'LastUpdated']
            for field in extra_fields:
                try:
                    value = stock.GetExtraData(field)
                    if value:
                        metadata[field] = value
                except AttributeError:
                    pass
            
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to get metadata for {symbol}: {e}")
            return None