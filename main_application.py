"""
Main Application Entry Point
Demonstrates the new modular architecture
"""

import sys
import os
import logging
from typing import Optional

from src.core.configuration_manager import ConfigurationManager
from src.orchestrators.import_orchestrator import ImportOrchestrator
from src.core.factory_classes import create_components_from_config, get_factory_status

logger = logging.getLogger(__name__)


def main():
    """Main execution function with new modular architecture"""
    
    # Configuration file path
    config_path = "config.ini"
    
    try:
        # Initialize configuration
        config = ConfigurationManager(config_path)
        
        # Print current configuration
        config.print_config()
        
        # Create components using factories
        logger.info("Creating components from configuration...")
        data_provider, exchange_mappers, database_adapter = create_components_from_config(config)
        
        # Print factory status
        factory_status = get_factory_status()
        logger.info("Factory Status:")
        for component_type, status in factory_status.items():
            logger.info(f"  {component_type}: {status['count']} available - {status['available']}")
        
        # Initialize orchestrator
        orchestrator = ImportOrchestrator(config_path)
        
        if not orchestrator.initialize(data_provider, exchange_mappers, database_adapter):
            logger.error("Failed to initialize orchestrator")
            return 1
        
        # Check database creation if needed
        database_path = config.get('DATABASE', 'database_path')
        create_if_not_exists = config.getboolean('DATABASE', 'create_if_not_exists')
        
        if not os.path.exists(database_path) and create_if_not_exists:
            logger.info(f"Database not found, creating: {database_path}")
            if database_adapter.create_database(database_path):
                logger.info("Database created successfully")
                # Reconnect to the new database
                if not database_adapter.connect(database_path):
                    logger.error("Failed to connect to newly created database")
                    return 1
            else:
                logger.error("Failed to create database")
                return 1
        
        # Run the import process
        logger.info("Starting import process...")
        result = orchestrator.run_import()
        
        if result.errors:
            logger.warning(f"Import completed with {len(result.errors)} errors")
            return 1
        else:
            logger.info("Import process completed successfully")
            return 0
        
    except Exception as e:
        logger.error(f"Application failed: {e}")
        return 1
    finally:
        # Cleanup
        try:
            if 'orchestrator' in locals():
                orchestrator.cleanup()
        except:
            pass


def create_sample_config():
    """Create a sample configuration file"""
    try:
        config_manager = ConfigurationManager("sample_config.ini")
        config_manager.create_default_config()
        print("Sample configuration created: sample_config.ini")
        print("Edit this file with your preferred settings, then rename to config.ini")
        return 0
    except Exception as e:
        print(f"Failed to create sample config: {e}")
        return 1


def validate_config(config_path: str = "config.ini"):
    """Validate configuration file"""
    try:
        config_manager = ConfigurationManager(config_path)
        config_manager.print_config()
        print(f"\nConfiguration file '{config_path}' is valid!")
        
        # Check database path
        db_path = config_manager.get('DATABASE', 'database_path')
        if os.path.exists(db_path):
            print(f"✓ Database exists: {db_path}")
        else:
            print(f"⚠ Database not found: {db_path}")
            if config_manager.getboolean('DATABASE', 'create_if_not_exists'):
                print("  (Will be created automatically)")
        
        # Check factory components
        try:
            from factory_classes import register_default_implementations
            register_default_implementations()
            factory_status = get_factory_status()
            
            print("\nAvailable Components:")
            for component_type, status in factory_status.items():
                print(f"  {component_type.title()}: {', '.join(status['available'])}")
        except Exception as e:
            print(f"⚠ Error checking components: {e}")
        
        return 0
        
    except Exception as e:
        print(f"Configuration validation failed: {e}")
        return 1


def list_components():
    """List available components"""
    try:
        from factory_classes import register_default_implementations
        register_default_implementations()
        
        factory_status = get_factory_status()
        
        print("Available Components:")
        print("=" * 40)
        
        for component_type, status in factory_status.items():
            print(f"\n{component_type.title()}:")
            for component in status['available']:
                print(f"  - {component}")
        
        return 0
        
    except Exception as e:
        print(f"Error listing components: {e}")
        return 1


def run_update_only():
    """Run update process only (no full import)"""
    try:
        config = ConfigurationManager()
        
        # Create components
        data_provider, exchange_mappers, database_adapter = create_components_from_config(config)
        
        # Initialize orchestrator
        orchestrator = ImportOrchestrator()
        if not orchestrator.initialize(data_provider, exchange_mappers, database_adapter):
            logger.error("Failed to initialize orchestrator")
            return 1
        
        # Run update only
        logger.info("Running update process...")
        if orchestrator.run_update():
            logger.info("Update process completed successfully")
            return 0
        else:
            logger.error("Update process failed")
            return 1
            
    except Exception as e:
        logger.error(f"Update process failed: {e}")
        return 1


def show_status():
    """Show current system status"""
    try:
        config = ConfigurationManager()
        
        print("System Status:")
        print("=" * 40)
        
        # Database status
        db_path = config.get('DATABASE', 'database_path')
        print(f"Database: {db_path}")
        if os.path.exists(db_path):
            print("  Status: ✓ Exists")
            # Could add more database stats here
        else:
            print("  Status: ✗ Not found")
        
        # Configuration status
        print(f"\nConfiguration:")
        print(f"  Data Provider: {config.get('PROVIDERS', 'data_provider')}")
        print(f"  Exchanges: {', '.join(config.getlist('PROVIDERS', 'exchanges'))}")
        print(f"  Database Adapter: {config.get('PROVIDERS', 'database_adapter')}")
        print(f"  Max Coins: {config.get('IMPORT', 'max_coins')}")
        print(f"  Min Market Cap: ${config.getfloat('IMPORT', 'min_market_cap'):,.0f}")
        
        # Component status
        factory_status = get_factory_status()
        print(f"\nAvailable Components:")
        for component_type, status in factory_status.items():
            print(f"  {component_type.title()}: {status['count']} registered")
        
        return 0
        
    except Exception as e:
        print(f"Error getting status: {e}")
        return 1


def print_help():
    """Print help information"""
    print("Crypto Data Importer - Modular Architecture")
    print("=" * 50)
    print("\nUsage: python main.py [command]")
    print("\nCommands:")
    print("  (no command)      - Run full import process")
    print("  create-config     - Create sample configuration file")
    print("  validate-config   - Validate configuration file")
    print("  list-components   - List available components")
    print("  update-only       - Run update process only")
    print("  status            - Show system status")
    print("  help              - Show this help message")
    print("\nExamples:")
    print("  python main.py                    # Run full import")
    print("  python main.py create-config      # Create config file")
    print("  python main.py validate-config    # Check config")
    print("  python main.py update-only        # Update existing data")


if __name__ == "__main__":
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "create-config":
            sys.exit(create_sample_config())
        elif command == "validate-config":
            config_file = sys.argv[2] if len(sys.argv) > 2 else "config.ini"
            sys.exit(validate_config(config_file))
        elif command == "list-components":
            sys.exit(list_components())
        elif command == "update-only":
            sys.exit(run_update_only())
        elif command == "status":
            sys.exit(show_status())
        elif command == "help" or command == "-h" or command == "--help":
            print_help()
            sys.exit(0)
        else:
            print(f"Unknown command: {command}")
            print("Use 'python main.py help' for available commands")
            sys.exit(1)
    else:
        # Normal execution - run full import
        sys.exit(main())
