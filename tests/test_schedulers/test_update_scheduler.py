"""
Test cases for UpdateScheduler
"""

import unittest
import tempfile
import os
import sys
import json
import threading
import time
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

# Add src to path
src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from schedulers.update_scheduler import UpdateScheduler
from core.configuration_manager import ConfigurationManager


class TestUpdateScheduler(unittest.TestCase):
    """Test cases for UpdateScheduler class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create mock configuration
        self.mock_config = Mock(spec=ConfigurationManager)
        self.mock_config.getint.return_value = 6  # Default update frequency
        self.mock_config.getboolean.return_value = True  # Default auto update enabled
        
        # Create temporary directory for state files
        self.temp_dir = tempfile.mkdtemp()
        self.test_state_file = os.path.join(self.temp_dir, "test_update_state.json")
        
        # Create scheduler with test state file
        self.scheduler = UpdateScheduler(self.mock_config)
        self.scheduler.update_state_file = self.test_state_file
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Clean up any threads
        if hasattr(self.scheduler, 'update_thread') and self.scheduler.update_thread:
            if self.scheduler.update_thread.is_alive():
                self.scheduler.update_thread.join(timeout=1.0)
        
        # Remove state file if exists
        if os.path.exists(self.test_state_file):
            os.remove(self.test_state_file)
        os.rmdir(self.temp_dir)
    
    def test_init_with_default_config(self):
        """Test initialization with default configuration"""
        self.mock_config.getint.side_effect = lambda section, key, default: {
            ('UPDATES', 'update_frequency_hours'): 6,
            ('UPDATES', 'update_days_back'): 7
        }.get((section, key), default)
        
        self.mock_config.getboolean.side_effect = lambda section, key, default: {
            ('UPDATES', 'auto_update_enabled'): True,
            ('UPDATES', 'update_on_startup'): True
        }.get((section, key), default)
        
        scheduler = UpdateScheduler(self.mock_config)
        
        self.assertEqual(scheduler.update_frequency_hours, 6)
        self.assertEqual(scheduler.update_days_back, 7)
        self.assertTrue(scheduler.auto_update_enabled)
        self.assertTrue(scheduler.update_on_startup)
        self.assertFalse(scheduler.is_updating)
        self.assertIsNone(scheduler.update_thread)
    
    def test_init_with_custom_config(self):
        """Test initialization with custom configuration"""
        self.mock_config.getint.side_effect = lambda section, key, default: {
            ('UPDATES', 'update_frequency_hours'): 12,
            ('UPDATES', 'update_days_back'): 14
        }.get((section, key), default)
        
        self.mock_config.getboolean.side_effect = lambda section, key, default: {
            ('UPDATES', 'auto_update_enabled'): False,
            ('UPDATES', 'update_on_startup'): False
        }.get((section, key), default)
        
        scheduler = UpdateScheduler(self.mock_config)
        
        self.assertEqual(scheduler.update_frequency_hours, 12)
        self.assertEqual(scheduler.update_days_back, 14)
        self.assertFalse(scheduler.auto_update_enabled)
        self.assertFalse(scheduler.update_on_startup)
    
    def test_should_update_auto_disabled(self):
        """Test should_update when auto update is disabled"""
        self.scheduler.auto_update_enabled = False
        
        result = self.scheduler.should_update()
        
        self.assertFalse(result)
    
    def test_should_update_already_updating(self):
        """Test should_update when already updating"""
        self.scheduler.is_updating = True
        
        result = self.scheduler.should_update()
        
        self.assertFalse(result)
    
    def test_should_update_no_previous_update(self):
        """Test should_update when no previous update exists"""
        self.scheduler.last_update = None
        
        result = self.scheduler.should_update()
        
        self.assertTrue(result)
    
    def test_should_update_time_elapsed(self):
        """Test should_update when enough time has elapsed"""
        # Set last update to 7 hours ago (frequency is 6 hours)
        self.scheduler.last_update = datetime.now() - timedelta(hours=7)
        self.scheduler.update_frequency_hours = 6
        
        result = self.scheduler.should_update()
        
        self.assertTrue(result)
    
    def test_should_update_time_not_elapsed(self):
        """Test should_update when not enough time has elapsed"""
        # Set last update to 3 hours ago (frequency is 6 hours)
        self.scheduler.last_update = datetime.now() - timedelta(hours=3)
        self.scheduler.update_frequency_hours = 6
        
        result = self.scheduler.should_update()
        
        self.assertFalse(result)
    
    def test_schedule_update_not_needed(self):
        """Test schedule_update when update is not needed"""
        # Make should_update return False
        with patch.object(self.scheduler, 'should_update', return_value=False):
            result = self.scheduler.schedule_update(lambda: True)
            
            self.assertFalse(result)
    
    def test_schedule_update_thread_already_running(self):
        """Test schedule_update when thread is already running"""
        # Create a mock running thread
        mock_thread = Mock()
        mock_thread.is_alive.return_value = True
        self.scheduler.update_thread = mock_thread
        
        with patch.object(self.scheduler, 'should_update', return_value=True):
            result = self.scheduler.schedule_update(lambda: True)
            
            self.assertFalse(result)
    
    @patch('threading.Thread')
    def test_schedule_update_success(self, mock_thread_class):
        """Test successful schedule_update"""
        mock_thread = Mock()
        mock_thread_class.return_value = mock_thread
        
        with patch.object(self.scheduler, 'should_update', return_value=True):
            callback = Mock(return_value=True)
            result = self.scheduler.schedule_update(callback, "arg1", kwarg1="value1")
            
            self.assertTrue(result)
            mock_thread_class.assert_called_once()
            mock_thread.start.assert_called_once()
            self.assertEqual(self.scheduler.update_thread, mock_thread)
    
    def test_run_scheduled_update_already_updating(self):
        """Test run_scheduled_update when already updating"""
        self.scheduler.is_updating = True
        
        result = self.scheduler.run_scheduled_update(lambda: True)
        
        self.assertFalse(result)
    
    def test_run_scheduled_update_success(self):
        """Test successful run_scheduled_update"""
        update_func = Mock(return_value=True)
        
        with patch.object(self.scheduler, '_mark_update_start') as mock_start, \
             patch.object(self.scheduler, '_mark_update_complete') as mock_complete:
            
            result = self.scheduler.run_scheduled_update(update_func, "arg1", kwarg1="value1")
            
            self.assertTrue(result)
            update_func.assert_called_once_with("arg1", kwarg1="value1")
            mock_start.assert_called_once()
            mock_complete.assert_called_once()
    
    def test_run_scheduled_update_failure(self):
        """Test run_scheduled_update when update function fails"""
        update_func = Mock(return_value=False)
        
        with patch.object(self.scheduler, '_mark_update_start') as mock_start:
            result = self.scheduler.run_scheduled_update(update_func)
            
            self.assertFalse(result)
            update_func.assert_called_once()
            mock_start.assert_called_once()
    
    def test_run_scheduled_update_exception(self):
        """Test run_scheduled_update when update function raises exception"""
        update_func = Mock(side_effect=Exception("Update failed"))
        
        with patch.object(self.scheduler, '_mark_update_start'):
            result = self.scheduler.run_scheduled_update(update_func)
            
            self.assertFalse(result)
    
    def test_run_scheduled_update_finally_block(self):
        """Test that is_updating is always reset in finally block"""
        update_func = Mock(side_effect=Exception("Update failed"))
        
        # Ensure is_updating is reset even after exception
        self.scheduler.run_scheduled_update(update_func)
        
        self.assertFalse(self.scheduler.is_updating)
    
    def test_update_specific_symbols_already_updating(self):
        """Test update_specific_symbols when already updating"""
        self.scheduler.is_updating = True
        
        result = self.scheduler.update_specific_symbols(["BTC", "ETH"], lambda: True)
        
        self.assertFalse(result)
    
    def test_update_specific_symbols_success(self):
        """Test successful update_specific_symbols"""
        symbols = ["BTC", "ETH", "ADA"]
        update_func = Mock(return_value=True)
        
        result = self.scheduler.update_specific_symbols(symbols, update_func, "extra_arg")
        
        self.assertTrue(result)
        update_func.assert_called_once_with("extra_arg", symbols=symbols)
    
    def test_update_specific_symbols_failure(self):
        """Test update_specific_symbols when update fails"""
        symbols = ["BTC", "ETH"]
        update_func = Mock(return_value=False)
        
        result = self.scheduler.update_specific_symbols(symbols, update_func)
        
        self.assertFalse(result)
    
    def test_update_specific_symbols_exception(self):
        """Test update_specific_symbols when exception occurs"""
        symbols = ["BTC"]
        update_func = Mock(side_effect=Exception("Update failed"))
        
        result = self.scheduler.update_specific_symbols(symbols, update_func)
        
        self.assertFalse(result)
        self.assertFalse(self.scheduler.is_updating)  # Should be reset
    
    def test_check_update_conditions_basic(self):
        """Test check_update_conditions basic functionality"""
        self.scheduler.auto_update_enabled = True
        self.scheduler.update_on_startup = True
        self.scheduler.last_update = datetime.now() - timedelta(hours=8)
        self.scheduler.update_frequency_hours = 6
        
        conditions = self.scheduler.check_update_conditions()
        
        self.assertTrue(conditions['auto_update_enabled'])
        self.assertTrue(conditions['time_based_update_needed'])
        self.assertFalse(conditions['startup_update_needed'])  # last_update exists
        self.assertFalse(conditions['manual_update_requested'])
        self.assertIsNotNone(conditions['last_update'])
        self.assertIsNotNone(conditions['hours_since_update'])
        self.assertFalse(conditions['is_updating'])
    
    def test_check_update_conditions_startup_needed(self):
        """Test check_update_conditions when startup update is needed"""
        self.scheduler.update_on_startup = True
        self.scheduler.last_update = None
        
        conditions = self.scheduler.check_update_conditions()
        
        self.assertTrue(conditions['startup_update_needed'])
        self.assertIsNone(conditions['last_update'])
        self.assertIsNone(conditions['hours_since_update'])
    
    def test_get_symbols_to_update_success(self):
        """Test get_symbols_to_update with mock database adapter"""
        mock_adapter = Mock()
        mock_adapter.get_symbol_list.return_value = ["BTC", "ETH", "ADA", "DOT"]
        
        symbols = self.scheduler.get_symbols_to_update(mock_adapter)
        
        self.assertEqual(symbols, ["BTC", "ETH", "ADA", "DOT"])
        mock_adapter.get_symbol_list.assert_called_once()
    
    def test_get_symbols_to_update_exception(self):
        """Test get_symbols_to_update when exception occurs"""
        mock_adapter = Mock()
        mock_adapter.get_symbol_list.side_effect = Exception("Database error")
        
        symbols = self.scheduler.get_symbols_to_update(mock_adapter)
        
        self.assertEqual(symbols, [])
    
    def test_mark_update_start(self):
        """Test _mark_update_start functionality"""
        initial_time = self.scheduler.last_update
        
        with patch.object(self.scheduler, '_save_update_state') as mock_save:
            self.scheduler._mark_update_start()
            
            self.assertIsNotNone(self.scheduler.last_update)
            self.assertNotEqual(self.scheduler.last_update, initial_time)
            mock_save.assert_called_once()
    
    def test_mark_update_complete(self):
        """Test _mark_update_complete functionality"""
        initial_time = self.scheduler.last_update
        
        with patch.object(self.scheduler, '_save_update_state') as mock_save:
            self.scheduler._mark_update_complete()
            
            self.assertIsNotNone(self.scheduler.last_update)
            self.assertNotEqual(self.scheduler.last_update, initial_time)
            mock_save.assert_called_once()
    
    def test_save_update_state_success(self):
        """Test successful _save_update_state"""
        self.scheduler.last_update = datetime.now()
        self.scheduler.update_frequency_hours = 12
        self.scheduler.auto_update_enabled = True
        
        self.scheduler._save_update_state()
        
        self.assertTrue(os.path.exists(self.test_state_file))
        
        with open(self.test_state_file, 'r') as f:
            data = json.load(f)
            
        self.assertIn('last_update', data)
        self.assertEqual(data['update_frequency_hours'], 12)
        self.assertTrue(data['auto_update_enabled'])
    
    def test_save_update_state_exception(self):
        """Test _save_update_state handles exceptions gracefully"""
        # Use invalid path to trigger exception
        self.scheduler.update_state_file = "/invalid/path/state.json"
        
        # Should not raise exception
        self.scheduler._save_update_state()
    
    def test_load_update_state_success(self):
        """Test successful _load_update_state"""
        # Create test state file
        test_time = datetime.now() - timedelta(hours=2)
        state_data = {
            'last_update': test_time.isoformat(),
            'update_frequency_hours': 8,
            'auto_update_enabled': False
        }
        
        with open(self.test_state_file, 'w') as f:
            json.dump(state_data, f)
        
        self.scheduler._load_update_state()
        
        self.assertIsNotNone(self.scheduler.last_update)
        self.assertEqual(self.scheduler.last_update.date(), test_time.date())
    
    def test_load_update_state_file_not_exists(self):
        """Test _load_update_state when file doesn't exist"""
        # Ensure file doesn't exist
        if os.path.exists(self.test_state_file):
            os.remove(self.test_state_file)
        
        # Should not raise exception
        self.scheduler._load_update_state()
        
        # last_update should remain None
        self.assertIsNone(self.scheduler.last_update)
    
    def test_load_update_state_invalid_json(self):
        """Test _load_update_state with invalid JSON"""
        # Create invalid JSON file
        with open(self.test_state_file, 'w') as f:
            f.write("invalid json content")
        
        # Should not raise exception
        self.scheduler._load_update_state()
    
    def test_force_update_check(self):
        """Test force_update_check functionality"""
        # Set recent last update
        self.scheduler.last_update = datetime.now() - timedelta(minutes=30)
        self.scheduler.update_frequency_hours = 6
        
        # Should not need update initially
        self.assertFalse(self.scheduler.should_update())
        
        # Force update check
        self.scheduler.force_update_check()
        
        # Should now need update
        self.assertTrue(self.scheduler.should_update())
    
    def test_disable_auto_updates(self):
        """Test disable_auto_updates functionality"""
        self.scheduler.auto_update_enabled = True
        
        self.scheduler.disable_auto_updates()
        
        self.assertFalse(self.scheduler.auto_update_enabled)
    
    def test_enable_auto_updates(self):
        """Test enable_auto_updates functionality"""
        self.scheduler.auto_update_enabled = False
        
        self.scheduler.enable_auto_updates()
        
        self.assertTrue(self.scheduler.auto_update_enabled)
    
    def test_set_update_frequency(self):
        """Test set_update_frequency functionality"""
        self.scheduler.set_update_frequency(12)
        
        self.assertEqual(self.scheduler.update_frequency_hours, 12)
    
    def test_set_update_frequency_minimum(self):
        """Test set_update_frequency enforces minimum of 1 hour"""
        self.scheduler.set_update_frequency(0)
        
        self.assertEqual(self.scheduler.update_frequency_hours, 1)
        
        self.scheduler.set_update_frequency(-5)
        
        self.assertEqual(self.scheduler.update_frequency_hours, 1)
    
    def test_get_next_update_time_disabled(self):
        """Test get_next_update_time when auto update is disabled"""
        self.scheduler.auto_update_enabled = False
        
        result = self.scheduler.get_next_update_time()
        
        self.assertIsNone(result)
    
    def test_get_next_update_time_no_last_update(self):
        """Test get_next_update_time when no last update"""
        self.scheduler.auto_update_enabled = True
        self.scheduler.last_update = None
        
        result = self.scheduler.get_next_update_time()
        
        self.assertIsNone(result)
    
    def test_get_next_update_time_success(self):
        """Test successful get_next_update_time"""
        self.scheduler.auto_update_enabled = True
        self.scheduler.last_update = datetime.now() - timedelta(hours=2)
        self.scheduler.update_frequency_hours = 6
        
        result = self.scheduler.get_next_update_time()
        
        self.assertIsNotNone(result)
        expected_time = self.scheduler.last_update + timedelta(hours=6)
        self.assertEqual(result.date(), expected_time.date())
        self.assertEqual(result.hour, expected_time.hour)
    
    def test_get_update_stats_complete(self):
        """Test get_update_stats with complete data"""
        self.scheduler.auto_update_enabled = True
        self.scheduler.update_frequency_hours = 8
        self.scheduler.last_update = datetime.now() - timedelta(hours=2)
        self.scheduler.is_updating = False
        
        stats = self.scheduler.get_update_stats()
        
        self.assertTrue(stats['auto_update_enabled'])
        self.assertEqual(stats['update_frequency_hours'], 8)
        self.assertIsNotNone(stats['last_update'])
        self.assertFalse(stats['is_updating'])
        self.assertIsNotNone(stats['next_update'])
        self.assertIn('hours', stats['time_until_next_update'])
    
    def test_get_update_stats_overdue(self):
        """Test get_update_stats when update is overdue"""
        self.scheduler.auto_update_enabled = True
        self.scheduler.last_update = datetime.now() - timedelta(hours=10)
        self.scheduler.update_frequency_hours = 6
        
        stats = self.scheduler.get_update_stats()
        
        self.assertEqual(stats['time_until_next_update'], "Overdue")
    
    def test_cleanup_old_state_recent_file(self):
        """Test cleanup_old_state with recent file"""
        # Create recent state file
        with open(self.test_state_file, 'w') as f:
            json.dump({'test': 'data'}, f)
        
        # Should not delete recent file
        self.scheduler.cleanup_old_state(days_to_keep=30)
        
        self.assertTrue(os.path.exists(self.test_state_file))
    
    def test_cleanup_old_state_old_file(self):
        """Test cleanup_old_state with old file"""
        # Create state file
        with open(self.test_state_file, 'w') as f:
            json.dump({'test': 'data'}, f)
        
        # Make file appear old by modifying timestamp
        old_time = time.time() - (35 * 24 * 3600)  # 35 days ago
        os.utime(self.test_state_file, (old_time, old_time))
        
        # Should refresh the file
        with patch.object(self.scheduler, '_save_update_state') as mock_save:
            self.scheduler.cleanup_old_state(days_to_keep=30)
            mock_save.assert_called_once()
    
    def test_cleanup_old_state_exception(self):
        """Test cleanup_old_state handles exceptions"""
        # Remove the file to trigger exception
        if os.path.exists(self.test_state_file):
            os.remove(self.test_state_file)
        
        # Should not raise exception
        self.scheduler.cleanup_old_state()


class TestUpdateSchedulerIntegration(unittest.TestCase):
    """Integration tests for UpdateScheduler"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.state_file = os.path.join(self.temp_dir, "integration_state.json")
        
        # Create real configuration
        from core.configuration_manager import ConfigurationManager
        
        config_content = f"""
[UPDATES]
auto_update_enabled = true
update_frequency_hours = 1
update_days_back = 3
update_on_startup = false
"""
        
        config_path = os.path.join(self.temp_dir, "config.ini")
        with open(config_path, 'w') as f:
            f.write(config_content)
        
        self.config = ConfigurationManager(config_path)
        self.scheduler = UpdateScheduler(self.config)
        self.scheduler.update_state_file = self.state_file
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Clean up threads
        if hasattr(self.scheduler, 'update_thread') and self.scheduler.update_thread:
            if self.scheduler.update_thread.is_alive():
                self.scheduler.update_thread.join(timeout=2.0)
        
        # Clean up files
        for file in os.listdir(self.temp_dir):
            file_path = os.path.join(self.temp_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        os.rmdir(self.temp_dir)
    
    def test_end_to_end_update_workflow(self):
        """Test complete update workflow"""
        update_called = threading.Event()
        update_results = []
        
        def mock_update_function(*args, **kwargs):
            update_results.append(('called', args, kwargs))
            update_called.set()
            return True
        
        # Force an update to be needed
        self.scheduler.force_update_check()
        
        # Schedule update
        result = self.scheduler.schedule_update(mock_update_function, "test_arg", test_kwarg="test_value")
        
        self.assertTrue(result)
        
        # Wait for update to complete
        update_completed = update_called.wait(timeout=5.0)
        self.assertTrue(update_completed, "Update function was not called within timeout")
        
        # Wait a bit more for thread cleanup
        if self.scheduler.update_thread:
            self.scheduler.update_thread.join(timeout=2.0)
        
        # Verify update was called with correct arguments
        self.assertEqual(len(update_results), 1)
        call_info = update_results[0]
        self.assertEqual(call_info[0], 'called')
        self.assertEqual(call_info[1], ("test_arg",))
        self.assertEqual(call_info[2], {"test_kwarg": "test_value"})
        
        # Verify state was updated
        self.assertIsNotNone(self.scheduler.last_update)
        self.assertFalse(self.scheduler.is_updating)
    
    def test_state_persistence(self):
        """Test that state is properly saved and loaded"""
        # Set some state
        test_time = datetime.now()
        self.scheduler.last_update = test_time
        self.scheduler.update_frequency_hours = 4
        self.scheduler.auto_update_enabled = False
        
        # Save state
        self.scheduler._save_update_state()
        
        # Create new scheduler instance
        new_scheduler = UpdateScheduler(self.config)
        new_scheduler.update_state_file = self.state_file
        new_scheduler._load_update_state()
        
        # Verify state was loaded
        self.assertIsNotNone(new_scheduler.last_update)
        self.assertEqual(new_scheduler.last_update.date(), test_time.date())
        self.assertEqual(new_scheduler.last_update.hour, test_time.hour)
    
    def test_concurrent_update_prevention(self):
        """Test that concurrent updates are prevented"""
        first_update_started = threading.Event()
        first_update_continue = threading.Event()
        update_call_count = 0
        
        def slow_update_function():
            nonlocal update_call_count
            update_call_count += 1
            first_update_started.set()
            first_update_continue.wait(timeout=5.0)
            return True
        
        # Force update to be needed
        self.scheduler.force_update_check()
        
        # Start first update
        result1 = self.scheduler.schedule_update(slow_update_function)
        self.assertTrue(result1)
        
        # Wait for first update to start
        first_update_started.wait(timeout=2.0)
        
        # Try to start second update (should be rejected)
        result2 = self.scheduler.schedule_update(slow_update_function)
        self.assertFalse(result2)
        
        # Allow first update to complete
        first_update_continue.set()
        
        # Wait for thread to complete
        if self.scheduler.update_thread:
            self.scheduler.update_thread.join(timeout=3.0)
        
        # Verify only one update was called
        self.assertEqual(update_call_count, 1)


if __name__ == '__main__':
    unittest.main()
