"""
Abstract Database Adapter Base Class
Defines the interface for database operations
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class AbstractDatabaseAdapter(ABC):
    """Abstract base class for database adapter implementations"""
    
    def __init__(self, config):
        self.config = config
        self.database_path = None
        self.connection_verified = False
    
    @abstractmethod
    def connect(self, database_path: str) -> bool:
        """Connect to the database
        
        Args:
            database_path: Path to the database
            
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def create_database(self, path: str) -> bool:
        """Create a new database
        
        Args:
            path: Path where to create the database
            
        Returns:
            True if creation successful, False otherwise
        """
        pass
    
    @abstractmethod
    def import_data(self, symbol: str, data: pd.DataFrame, metadata: Dict = None) -> bool:
        """Import new data for a symbol
        
        Args:
            symbol: Trading symbol
            data: DataFrame containing OHLCV data
            metadata: Additional metadata for the symbol
            
        Returns:
            True if import successful, False otherwise
        """
        pass
    
    @abstractmethod
    def update_data(self, symbol: str, data: pd.DataFrame) -> Tuple[int, int]:
        """Update existing data for a symbol
        
        Args:
            symbol: Trading symbol
            data: DataFrame containing new/updated OHLCV data
            
        Returns:
            Tuple of (new_records_added, existing_records_updated)
        """
        pass
    
    @abstractmethod
    def get_existing_range(self, symbol: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Get the date range of existing data for a symbol
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Tuple of (start_date, end_date) or (None, None) if no data exists
        """
        pass
    
    @abstractmethod
    def create_groups(self) -> bool:
        """Create organizational groups in the database
        
        Returns:
            True if groups created successfully, False otherwise
        """
        pass
    
    def validate_connection(self) -> bool:
        """Validate the current database connection
        
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            # Default implementation - subclasses can override
            return self.connection_verified
        except Exception as e:
            logger.error(f"Connection validation failed: {e}")
            return False
    
    def get_symbol_list(self) -> List[str]:
        """Get list of all symbols in the database
        
        Returns:
            List of symbol strings
        """
        # Default implementation - subclasses should override
        logger.warning("get_symbol_list not implemented in base class")
        return []
    
    def symbol_exists(self, symbol: str) -> bool:
        """Check if a symbol exists in the database
        
        Args:
            symbol: Trading symbol to check
            
        Returns:
            True if symbol exists, False otherwise
        """
        symbols = self.get_symbol_list()
        return symbol in symbols
    
    def delete_symbol(self, symbol: str) -> bool:
        """Delete a symbol and all its data from the database
        
        Args:
            symbol: Trading symbol to delete
            
        Returns:
            True if deletion successful, False otherwise
        """
        # Default implementation - subclasses should override
        logger.warning("delete_symbol not implemented in base class")
        return False
    
    def get_symbol_metadata(self, symbol: str) -> Optional[Dict]:
        """Get metadata for a symbol
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Dictionary containing symbol metadata or None if not found
        """
        # Default implementation - subclasses should override
        logger.warning("get_symbol_metadata not implemented in base class")
        return None
    
    def set_symbol_metadata(self, symbol: str, metadata: Dict) -> bool:
        """Set metadata for a symbol
        
        Args:
            symbol: Trading symbol
            metadata: Dictionary containing metadata to set
            
        Returns:
            True if successful, False otherwise
        """
        # Default implementation - subclasses should override
        logger.warning("set_symbol_metadata not implemented in base class")
        return False
    
    def backup_database(self, backup_path: str = None) -> bool:
        """Create a backup of the database
        
        Args:
            backup_path: Optional custom backup path
            
        Returns:
            True if backup successful, False otherwise
        """
        # Default implementation - subclasses should override
        logger.warning("backup_database not implemented in base class")
        return False
    
    def get_data_range(self, symbol: str, start_date: datetime = None, 
                      end_date: datetime = None) -> Optional[pd.DataFrame]:
        """Get data for a symbol within a date range
        
        Args:
            symbol: Trading symbol
            start_date: Start date for data range
            end_date: End date for data range
            
        Returns:
            DataFrame containing OHLCV data or None if not found
        """
        # Default implementation - subclasses should override
        logger.warning("get_data_range not implemented in base class")
        return None
    
    def get_latest_data(self, symbol: str, num_records: int = 1) -> Optional[pd.DataFrame]:
        """Get the latest N records for a symbol
        
        Args:
            symbol: Trading symbol
            num_records: Number of latest records to retrieve
            
        Returns:
            DataFrame containing latest OHLCV data or None if not found
        """
        # Default implementation - subclasses should override
        logger.warning("get_latest_data not implemented in base class")
        return None
    
    def validate_data_format(self, data: pd.DataFrame) -> bool:
        """Validate that data format is correct for import
        
        Args:
            data: DataFrame to validate
            
        Returns:
            True if format is valid, False otherwise
        """
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        if not isinstance(data, pd.DataFrame):
            logger.error("Data must be a pandas DataFrame")
            return False
        
        if data.empty:
            logger.error("Data DataFrame is empty")
            return False
        
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        # Check for valid datetime index
        if not isinstance(data.index, pd.DatetimeIndex):
            logger.error("Data must have a DatetimeIndex")
            return False
        
        # Check for numeric data types
        for col in required_columns:
            if not pd.api.types.is_numeric_dtype(data[col]):
                logger.error(f"Column {col} must be numeric")
                return False
        
        return True
    
    def get_database_stats(self) -> Dict:
        """Get statistics about the database
        
        Returns:
            Dictionary containing database statistics
        """
        try:
            symbols = self.get_symbol_list()
            stats = {
                'total_symbols': len(symbols),
                'database_path': self.database_path,
                'connection_status': self.connection_verified
            }
            
            if symbols:
                # Get data range for first symbol as sample
                sample_symbol = symbols[0]
                start_date, end_date = self.get_existing_range(sample_symbol)
                if start_date and end_date:
                    stats['sample_date_range'] = {
                        'start': start_date.isoformat(),
                        'end': end_date.isoformat(),
                        'symbol': sample_symbol
                    }
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {'error': str(e)}
    
    def cleanup_old_data(self, days_to_keep: int = 365) -> int:
        """Clean up old data beyond specified days
        
        Args:
            days_to_keep: Number of days of data to keep
            
        Returns:
            Number of records cleaned up
        """
        # Default implementation - subclasses should override
        logger.warning("cleanup_old_data not implemented in base class")
        return 0
    
    def optimize_database(self) -> bool:
        """Optimize database performance
        
        Returns:
            True if optimization successful, False otherwise
        """
        # Default implementation - subclasses should override
        logger.warning("optimize_database not implemented in base class")
        return False
