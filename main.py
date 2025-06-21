"""
Main Entry Point for Crypto Data Importer
Updated for proper package structure with improved error handling and security
"""

import sys
import os
import logging
import functools
from pathlib import Path
from typing import Tuple, Optional, Any

# Add src to Python path for development with security validation
try:
    src_path = Path(__file__).resolve().parent / "src"
    if src_path.exists() and src_path.is_dir():
        # Ensure we're not adding potentially unsafe paths
        if str(src_path).count('..') == 0:  # Prevent directory traversal
            sys.path.insert(0, str(src_path))
        else:
            raise ValueError("Unsafe path detected")
except (OSError, ValueError) as e:
    print(f"Error setting up source path: {e}")
    sys.exit(1)

from core.configuration_manager import ConfigurationManager
from orchestrators.import_orchestrator import ImportOrchestrator
from core.factory_classes import create_components_from_config, get_factory_status

logger = logging.getLogger(__name__)


class ExitCodes:
    """Exit code constants for consistent error reporting"""
    SUCCESS = 0
    GENERAL_ERROR = 1
    CONFIG_ERROR = 2
    DATABASE_ERROR = 3
    INITIALIZATION_ERROR = 4


class SystemOutput:
    """Unified output system for consistent messaging"""
    
    @staticmethod
    def info(message: str, use_logger: bool = True):
        """Output info message"""
        if use_logger and logger.isEnabledFor(logging.INFO):
            logger.info(message)
        else:
            print(f"INFO: {message}")
    
    @staticmethod
    def warning(message: str, use_logger: bool = True):
        """Output warning message"""
        if use_logger and logger.isEnabledFor(logging.WARNING):
            logger.warning(message)
        else:
            print(f"WARNING: {message}")
    
    @staticmethod
    def error(message: str, use_logger: bool = True):
        """Output error message"""
        if use_logger and logger.isEnabledFor(logging.ERROR):
            logger.error(message)
        else:
            print(f"ERROR: {message}")
    
    @staticmethod
    def success(message: str, use_logger: bool = True):
        """Output success message"""
        if use_logger and logger.isEnabledFor(logging.INFO):
            logger.info(message)
        else:
            print(f"✓ {message}")


def validate_path(path: str, must_exist: bool = False, must_be_file: bool = False) -> bool:
    """Validate file paths for security and existence
    
    Args:
        path: Path to validate
        must_exist: Whether the path must already exist
        must_be_file: Whether the path must be a file (if it exists)
        
    Returns:
        True if path is valid, False otherwise
    """
    try:
        resolved_path = Path(path).resolve()
        
        # Security check: prevent directory traversal
        if '..' in str(resolved_path) or str(resolved_path).count('..') > 0:
            return False
        
        if must_exist and not resolved_path.exists():
            return False
            
        if must_be_file and resolved_path.exists() and not resolved_path.is_file():
            return False
            
        return True
    except (OSError, ValueError, PermissionError):
        return False


@functools.lru_cache(maxsize=1)
def ensure_factories_registered() -> None:
    """Ensure factory implementations are registered (cached)"""
    try:
        from core.factory_classes import register_default_implementations
        register_default_implementations()
    except ImportError as e:
        SystemOutput.error(f"Failed to register factory implementations: {e}")
        raise


def initialize_system(config_path: str = "config.ini") -> Tuple[ConfigurationManager, Tuple[Any, Any, Any]]:
    """Common initialization logic with proper validation
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Tuple of (config_manager, (data_provider, exchange_mappers, database_adapter))
        
    Raises:
        FileNotFoundError: If configuration file doesn't exist
        ValueError: If configuration is invalid
        RuntimeError: If component creation fails
    """
    # Validate configuration file path
    if not validate_path(config_path, must_exist=True, must_be_file=True):
        raise FileNotFoundError(f"Configuration file not found or invalid: {config_path}")
    
    # Initialize configuration
    try:
        config = ConfigurationManager(config_path)
    except Exception as e:
        raise ValueError(f"Failed to load configuration: {e}")
    
    # Ensure factories are registered
    ensure_factories_registered()
    
    # Create components using factories
    try:
        SystemOutput.info("Creating components from configuration...")
        components = create_components_from_config(config)
        if components is None or len(components) != 3:
            raise RuntimeError("Component creation returned invalid result")
        
        data_provider, exchange_mappers, database_adapter = components
        
        if data_provider is None or database_adapter is None:
            raise RuntimeError("Failed to create required components")
            
        return config, (data_provider, exchange_mappers, database_adapter)
        
    except Exception as e:
        raise RuntimeError(f"Failed to create components: {e}")


def cleanup_resources(*resources) -> None:
    """Clean up resources with proper error handling
    
    Args:
        *resources: Variable number of resources to clean up
    """
    cleanup_errors = []
    
    for i, resource in enumerate(resources):
        if resource is None:
            continue
            
        resource_name = f"resource_{i}"
        try:
            # Try different cleanup method names
            if hasattr(resource, 'cleanup'):
                resource.cleanup()
            elif hasattr(resource, 'close'):
                resource.close()
            elif hasattr(resource, 'disconnect'):
                resource.disconnect()
        except Exception as e:
            cleanup_errors.append(f"{resource_name}: {e}")
    
    if cleanup_errors:
        SystemOutput.warning(f"Cleanup errors: {'; '.join(cleanup_errors)}")


def main() -> int:
    """Main execution function with modular architecture"""
    
    config_path = "config.ini"
    orchestrator = None
    data_provider = None
    database_adapter = None
    
    try:
        # Check if configuration file exists
        if not os.path.exists(config_path):
            SystemOutput.error(f"Configuration file not found: {config_path}")
            SystemOutput.info("Run 'python main.py create-config' to create a sample configuration")
            return ExitCodes.CONFIG_ERROR
        
        # Initialize system components
        try:
            config, (data_provider, exchange_mappers, database_adapter) = initialize_system(config_path)
        except FileNotFoundError as e:
            SystemOutput.error(str(e))
            return ExitCodes.CONFIG_ERROR
        except (ValueError, RuntimeError) as e:
            SystemOutput.error(str(e))
            return ExitCodes.INITIALIZATION_ERROR
        
        # Print current configuration
        config.print_config()
        
        # Print factory status
        factory_status = get_factory_status()
        SystemOutput.info("Factory Status:")
        for component_type, status in factory_status.items():
            SystemOutput.info(f"  {component_type}: {status['count']} available - {status['available']}")
        
        # Initialize orchestrator
        orchestrator = ImportOrchestrator(config_path)
        
        if not orchestrator.initialize(data_provider, exchange_mappers, database_adapter):
            SystemOutput.error("Failed to initialize orchestrator")
            return ExitCodes.INITIALIZATION_ERROR
        
        # Check database creation if needed
        database_path = config.get('DATABASE', 'database_path')
        
        # Validate database path
        if not validate_path(database_path):
            SystemOutput.error(f"Invalid database path: {database_path}")
            return ExitCodes.DATABASE_ERROR
        
        create_if_not_exists = config.getboolean('DATABASE', 'create_if_not_exists')
        
        if not os.path.exists(database_path) and create_if_not_exists:
            SystemOutput.info(f"Database not found, creating: {database_path}")
            try:
                if database_adapter.create_database(database_path):
                    SystemOutput.success("Database created successfully")
                    # Reconnect to the new database
                    if not database_adapter.connect(database_path):
                        SystemOutput.error("Failed to connect to newly created database")
                        return ExitCodes.DATABASE_ERROR
                else:
                    SystemOutput.error("Failed to create database")
                    return ExitCodes.DATABASE_ERROR
            except Exception as e:
                SystemOutput.error(f"Database creation failed: {e}")
                return ExitCodes.DATABASE_ERROR
        
        # Run the import process
        SystemOutput.info("Starting import process...")
        result = orchestrator.run_import()
        
        # Validate import result
        if result is None:
            SystemOutput.error("Import process returned no result")
            return ExitCodes.GENERAL_ERROR
        
        if result.errors:
            SystemOutput.warning(f"Import completed with {len(result.errors)} errors")
            for error in result.errors:
                SystemOutput.error(f"  - {error}")
            return ExitCodes.GENERAL_ERROR
        else:
            SystemOutput.success("Import process completed successfully")
            return ExitCodes.SUCCESS
        
    except FileNotFoundError as e:
        SystemOutput.error(f"File not found: {e}")
        return ExitCodes.CONFIG_ERROR
    except PermissionError as e:
        SystemOutput.error(f"Permission denied: {e}")
        return ExitCodes.GENERAL_ERROR
    except Exception as e:
        SystemOutput.error(f"Unexpected error during import: {e}")
        logger.exception("Full traceback:")  # Log full traceback for debugging
        return ExitCodes.GENERAL_ERROR
    finally:
        # Comprehensive cleanup
        cleanup_resources(orchestrator, database_adapter, data_provider)


def create_sample_config() -> int:
    """Create a sample configuration file"""
    try:
        config_manager = ConfigurationManager("sample_config.ini")
        config_manager.create_default_config()
        SystemOutput.success("Sample configuration created: sample_config.ini")
        SystemOutput.info("Edit this file with your preferred settings, then rename to config.ini")
        return ExitCodes.SUCCESS
    except PermissionError as e:
        SystemOutput.error(f"Permission denied creating config file: {e}")
        return ExitCodes.GENERAL_ERROR
    except Exception as e:
        SystemOutput.error(f"Failed to create sample config: {e}")
        return ExitCodes.GENERAL_ERROR


def validate_config(config_path: str = "config.ini") -> int:
    """Validate configuration file"""
    try:
        # Validate path first
        if not validate_path(config_path, must_exist=True, must_be_file=True):
            SystemOutput.error(f"Configuration file not found or invalid: {config_path}")
            return ExitCodes.CONFIG_ERROR
        
        config_manager = ConfigurationManager(config_path)
        config_manager.print_config()
        SystemOutput.success(f"Configuration file '{config_path}' is valid!")
        
        # Check database path
        db_path = config_manager.get('DATABASE', 'database_path')
        if validate_path(db_path, must_exist=True):
            SystemOutput.success(f"Database exists: {db_path}")
        else:
            SystemOutput.warning(f"Database not found: {db_path}")
            if config_manager.getboolean('DATABASE', 'create_if_not_exists'):
                SystemOutput.info("  (Will be created automatically)")
        
        # Check factory components
        try:
            ensure_factories_registered()
            factory_status = get_factory_status()
            
            SystemOutput.info("\nAvailable Components:")
            for component_type, status in factory_status.items():
                SystemOutput.info(f"  {component_type.title()}: {', '.join(status['available'])}")
        except Exception as e:
            SystemOutput.warning(f"Error checking components: {e}")
        
        return ExitCodes.SUCCESS
        
    except FileNotFoundError as e:
        SystemOutput.error(f"Configuration file error: {e}")
        return ExitCodes.CONFIG_ERROR
    except Exception as e:
        SystemOutput.error(f"Configuration validation failed: {e}")
        return ExitCodes.CONFIG_ERROR


def list_components() -> int:
    """List available components"""
    try:
        ensure_factories_registered()
        
        factory_status = get_factory_status()
        
        SystemOutput.info("Available Components:")
        SystemOutput.info("=" * 40)
        
        for component_type, status in factory_status.items():
            SystemOutput.info(f"\n{component_type.title()}:")
            for component in status['available']:
                SystemOutput.info(f"  - {component}")
        
        return ExitCodes.SUCCESS
        
    except Exception as e:
        SystemOutput.error(f"Error listing components: {e}")
        return ExitCodes.GENERAL_ERROR


def run_update_only(config_path: str = "config.ini") -> int:
    """Run update process only (no full import)"""
    
    orchestrator = None
    data_provider = None
    database_adapter = None
    
    try:
        # Initialize system components
        try:
            config, (data_provider, exchange_mappers, database_adapter) = initialize_system(config_path)
        except (FileNotFoundError, ValueError, RuntimeError) as e:
            SystemOutput.error(str(e))
            return ExitCodes.INITIALIZATION_ERROR
        
        # Initialize orchestrator
        orchestrator = ImportOrchestrator(config_path)
        if not orchestrator.initialize(data_provider, exchange_mappers, database_adapter):
            SystemOutput.error("Failed to initialize orchestrator")
            return ExitCodes.INITIALIZATION_ERROR
        
        # Run update only
        SystemOutput.info("Running update process...")
        if orchestrator.run_update():
            SystemOutput.success("Update process completed successfully")
            return ExitCodes.SUCCESS
        else:
            SystemOutput.error("Update process failed")
            return ExitCodes.GENERAL_ERROR
            
    except Exception as e:
        SystemOutput.error(f"Update process failed: {e}")
        return ExitCodes.GENERAL_ERROR
    finally:
        # Cleanup resources
        cleanup_resources(orchestrator, database_adapter, data_provider)


def show_status(config_path: str = "config.ini") -> int:
    """Show current system status"""
    try:
        # Validate configuration file first
        if not validate_path(config_path, must_exist=True, must_be_file=True):
            SystemOutput.error(f"Configuration file not found: {config_path}")
            return ExitCodes.CONFIG_ERROR
        
        config = ConfigurationManager(config_path)
        
        SystemOutput.info("System Status:")
        SystemOutput.info("=" * 40)
        
        # Database status
        db_path = config.get('DATABASE', 'database_path')
        SystemOutput.info(f"Database: {db_path}")
        if validate_path(db_path, must_exist=True):
            SystemOutput.info("  Status: ✓ Exists")
        else:
            SystemOutput.info("  Status: ✗ Not found")
        
        # Configuration status
        SystemOutput.info(f"\nConfiguration:")
        SystemOutput.info(f"  Data Provider: {config.get('PROVIDERS', 'data_provider')}")
        SystemOutput.info(f"  Exchanges: {', '.join(config.getlist('PROVIDERS', 'exchanges'))}")
        SystemOutput.info(f"  Database Adapter: {config.get('PROVIDERS', 'database_adapter')}")
        SystemOutput.info(f"  Max Coins: {config.get('IMPORT', 'max_coins')}")
        SystemOutput.info(f"  Min Market Cap: ${config.getfloat('IMPORT', 'min_market_cap'):,.0f}")
        
        # Component status
        ensure_factories_registered()
        factory_status = get_factory_status()
        SystemOutput.info(f"\nAvailable Components:")
        for component_type, status in factory_status.items():
            SystemOutput.info(f"  {component_type.title()}: {status['count']} registered")
        
        return ExitCodes.SUCCESS
        
    except Exception as e:
        SystemOutput.error(f"Error getting status: {e}")
        return ExitCodes.GENERAL_ERROR


def print_help() -> None:
    """Print help information"""
    help_text = """Crypto Data Importer - Modular Architecture
==================================================

Usage: python main.py [command] [options]

Commands:
  (no command)      - Run full import process
  create-config     - Create sample configuration file
  validate-config   - Validate configuration file
  list-components   - List available components
  update-only       - Run update process only
  status            - Show system status
  help              - Show this help message

Options:
  For validate-config: [config_file_path]

Examples:
  python main.py                                    # Run full import
  python main.py create-config                      # Create config file
  python main.py validate-config                    # Check default config
  python main.py validate-config custom_config.ini # Check custom config
  python main.py update-only                        # Update existing data
  python main.py status                             # Show system status

Exit Codes:
  0 - Success
  1 - General error
  2 - Configuration error
  3 - Database error
  4 - Initialization error

For more information, see the documentation in CLAUDE.md
"""
    print(help_text)


if __name__ == "__main__":
    
    # Parse command line arguments with improved validation
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "create-config":
            sys.exit(create_sample_config())
        elif command == "validate-config":
            config_file = "config.ini"
            if len(sys.argv) > 2:
                config_file = sys.argv[2]
                if not os.path.exists(config_file):
                    SystemOutput.error(f"Configuration file not found: {config_file}")
                    sys.exit(ExitCodes.CONFIG_ERROR)
            sys.exit(validate_config(config_file))
        elif command == "list-components":
            sys.exit(list_components())
        elif command == "update-only":
            sys.exit(run_update_only())
        elif command == "status":
            sys.exit(show_status())
        elif command in ["help", "-h", "--help"]:
            print_help()
            sys.exit(ExitCodes.SUCCESS)
        else:
            SystemOutput.error(f"Unknown command: {command}")
            SystemOutput.info("Use 'python main.py help' for available commands")
            sys.exit(ExitCodes.GENERAL_ERROR)
    else:
        # Normal execution - run full import
        sys.exit(main())