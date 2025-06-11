"""
Update Scheduler for Crypto Data Importer
Manages automatic updates and scheduling of data refresh operations
"""

import time
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import logging
import threading
import json
import os

logger = logging.getLogger(__name__)


class UpdateScheduler:
    """Manages scheduling and execution of data updates"""
    
    def __init__(self, config):
        self.config = config
        self.last_update = None
        self.update_frequency_hours = config.getint('UPDATES', 'update_frequency_hours', 6)
        self.auto_update_enabled = config.getboolean('UPDATES', 'auto_update_enabled', True)
        self.update_days_back = config.getint('UPDATES', 'update_days_back', 7)
        self.update_on_startup = config.getboolean('UPDATES', 'update_on_startup', True)
        
        # State tracking
        self.update_state_file = "update_state.json"
        self.is_updating = False
        self.update_thread = None
        
        # Load last update time
        self._load_update_state()
    
    def should_update(self) -> bool:
        """Check if an update should be performed based on schedule"""
        if not self.auto_update_enabled:
            return False
        
        if self.is_updating:
            logger.debug("Update already in progress")
            return False
        
        if self.last_update is None:
            logger.info("No previous update found, update needed")
            return True
        
        time_since_update = datetime.now() - self.last_update
        hours_since_update = time_since_update.total_seconds() / 3600
        
        if hours_since_update >= self.update_frequency_hours:
            logger.info(f"Update needed: {hours_since_update:.1f} hours since last update")
            return True
        
        logger.debug(f"Update not needed: {hours_since_update:.1f} hours since last update")
        return False
    
    def schedule_update(self, callback_func, *args, **kwargs) -> bool:
        """Schedule an update to run in the background
        
        Args:
            callback_func: Function to call for the update
            *args, **kwargs: Arguments to pass to the callback function
            
        Returns:
            True if update was scheduled, False otherwise
        """
        if not self.should_update():
            return False
        
        if self.update_thread and self.update_thread.is_alive():
            logger.warning("Update thread already running")
            return False
        
        logger.info("Scheduling background update")
        
        def update_worker():
            try:
                self.is_updating = True
                self._mark_update_start()
                
                # Execute the update callback
                result = callback_func(*args, **kwargs)
                
                if result:
                    self._mark_update_complete()
                    logger.info("Scheduled update completed successfully")
                else:
                    logger.error("Scheduled update failed")
                    
            except Exception as e:
                logger.error(f"Error in scheduled update: {e}")
            finally:
                self.is_updating = False
        
        self.update_thread = threading.Thread(target=update_worker, daemon=True)
        self.update_thread.start()
        
        return True
    
    def run_scheduled_update(self, update_func, *args, **kwargs) -> bool:
        """Run a scheduled update immediately (blocking)
        
        Args:
            update_func: Function to call for the update
            *args, **kwargs: Arguments to pass to the update function
            
        Returns:
            True if update successful, False otherwise
        """
        if self.is_updating:
            logger.warning("Update already in progress")
            return False
        
        try:
            self.is_updating = True
            self._mark_update_start()
            
            logger.info("Running scheduled update")
            result = update_func(*args, **kwargs)
            
            if result:
                self._mark_update_complete()
                logger.info("Scheduled update completed successfully")
                return True
            else:
                logger.error("Scheduled update failed")
                return False
                
        except Exception as e:
            logger.error(f"Error in scheduled update: {e}")
            return False
        finally:
            self.is_updating = False
    
    def update_specific_symbols(self, symbols: List[str], update_func, *args, **kwargs) -> bool:
        """Update specific symbols only
        
        Args:
            symbols: List of symbols to update
            update_func: Function to call for the update
            *args, **kwargs: Additional arguments for update function
            
        Returns:
            True if update successful, False otherwise
        """
        if self.is_updating:
            logger.warning("Update already in progress")
            return False
        
        try:
            self.is_updating = True
            logger.info(f"Updating specific symbols: {symbols}")
            
            # Call update function with specific symbols
            result = update_func(symbols=symbols, *args, **kwargs)
            
            if result:
                logger.info(f"Symbol update completed for {len(symbols)} symbols")
                return True
            else:
                logger.error("Symbol update failed")
                return False
                
        except Exception as e:
            logger.error(f"Error updating specific symbols: {e}")
            return False
        finally:
            self.is_updating = False
    
    def check_update_conditions(self) -> Dict:
        """Check various conditions that might trigger an update
        
        Returns:
            Dictionary containing update condition status
        """
        conditions = {
            'auto_update_enabled': self.auto_update_enabled,
            'time_based_update_needed': False,
            'startup_update_needed': False,
            'manual_update_requested': False,
            'last_update': self.last_update.isoformat() if self.last_update else None,
            'hours_since_update': None,
            'is_updating': self.is_updating
        }
        
        # Check time-based update
        if self.last_update:
            time_since_update = datetime.now() - self.last_update
            hours_since_update = time_since_update.total_seconds() / 3600
            conditions['hours_since_update'] = hours_since_update
            conditions['time_based_update_needed'] = hours_since_update >= self.update_frequency_hours
        
        # Check startup update
        if self.update_on_startup and self.last_update is None:
            conditions['startup_update_needed'] = True
        
        return conditions
    
    def get_symbols_to_update(self, database_adapter) -> List[str]:
        """Get list of symbols that need updating
        
        Args:
            database_adapter: Database adapter to query symbols
            
        Returns:
            List of symbols that need updating
        """
        try:
            all_symbols = database_adapter.get_symbol_list()
            
            # For now, return all symbols
            # Could be enhanced to only return symbols that actually need updates
            # based on last update time, data gaps, etc.
            
            logger.info(f"Found {len(all_symbols)} symbols for potential update")
            return all_symbols
            
        except Exception as e:
            logger.error(f"Error getting symbols to update: {e}")
            return []
    
    def _mark_update_start(self):
        """Mark the start of an update operation"""
        self.last_update = datetime.now()
        self._save_update_state()
    
    def _mark_update_complete(self):
        """Mark the completion of an update operation"""
        self.last_update = datetime.now()
        self._save_update_state()
    
    def _save_update_state(self):
        """Save update state to file"""
        try:
            state = {
                'last_update': self.last_update.isoformat() if self.last_update else None,
                'update_frequency_hours': self.update_frequency_hours,
                'auto_update_enabled': self.auto_update_enabled
            }
            
            with open(self.update_state_file, 'w') as f:
                json.dump(state, f, indent=2)
                
        except Exception as e:
            logger.debug(f"Failed to save update state: {e}")
    
    def _load_update_state(self):
        """Load update state from file"""
        try:
            if os.path.exists(self.update_state_file):
                with open(self.update_state_file, 'r') as f:
                    state = json.load(f)
                
                last_update_str = state.get('last_update')
                if last_update_str:
                    self.last_update = datetime.fromisoformat(last_update_str)
                
                logger.info(f"Loaded update state: last update {self.last_update}")
            
        except Exception as e:
            logger.debug(f"Failed to load update state: {e}")
    
    def force_update_check(self):
        """Force an immediate update check regardless of schedule"""
        logger.info("Forcing update check")
        self.last_update = datetime.now() - timedelta(hours=self.update_frequency_hours + 1)
    
    def disable_auto_updates(self):
        """Disable automatic updates"""
        self.auto_update_enabled = False
        logger.info("Automatic updates disabled")
    
    def enable_auto_updates(self):
        """Enable automatic updates"""
        self.auto_update_enabled = True
        logger.info("Automatic updates enabled")
    
    def set_update_frequency(self, hours: int):
        """Set update frequency in hours
        
        Args:
            hours: Number of hours between updates
        """
        self.update_frequency_hours = max(1, hours)  # Minimum 1 hour
        logger.info(f"Update frequency set to {self.update_frequency_hours} hours")
    
    def get_next_update_time(self) -> Optional[datetime]:
        """Get the time when the next update is scheduled
        
        Returns:
            DateTime of next scheduled update or None if not scheduled
        """
        if not self.auto_update_enabled or not self.last_update:
            return None
        
        next_update = self.last_update + timedelta(hours=self.update_frequency_hours)
        return next_update
    
    def get_update_stats(self) -> Dict:
        """Get statistics about update operations
        
        Returns:
            Dictionary containing update statistics
        """
        stats = {
            'auto_update_enabled': self.auto_update_enabled,
            'update_frequency_hours': self.update_frequency_hours,
            'last_update': self.last_update.isoformat() if self.last_update else None,
            'is_updating': self.is_updating,
            'next_update': None,
            'time_until_next_update': None
        }
        
        next_update = self.get_next_update_time()
        if next_update:
            stats['next_update'] = next_update.isoformat()
            time_until = next_update - datetime.now()
            if time_until.total_seconds() > 0:
                stats['time_until_next_update'] = f"{time_until.total_seconds() / 3600:.1f} hours"
            else:
                stats['time_until_next_update'] = "Overdue"
        
        return stats
    
    def cleanup_old_state(self, days_to_keep: int = 30):
        """Clean up old update state files
        
        Args:
            days_to_keep: Number of days of state to keep
        """
        try:
            # This could be enhanced to keep historical update logs
            # For now, just ensure the current state file is valid
            if os.path.exists(self.update_state_file):
                file_age = datetime.now() - datetime.fromtimestamp(
                    os.path.getmtime(self.update_state_file)
                )
                
                if file_age.days > days_to_keep:
                    logger.info("Cleaning up old update state")
                    self._save_update_state()  # Refresh the file
                    
        except Exception as e:
            logger.debug(f"Error cleaning up update state: {e}")
