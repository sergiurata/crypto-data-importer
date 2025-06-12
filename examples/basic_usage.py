"""
Basic Usage Example for Crypto Data Importer
Demonstrates simple import workflow
"""

import sys
import os
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from core.configuration_manager import ConfigurationManager
from orchestrators.import_orchestrator import ImportOrchestrator
from core.factory_classes import create_components_from_config


def basic_import_example():
    """Basic example of importing cryptocurrency data"""
    
    print("=== Crypto Data Importer - Basic Usage ===\n")
    
    # 1. Create configuration
    print("1. Setting up configuration...")
    config = ConfigurationManager("examples/example_config.ini")
    
    # Override some settings for this example
    config.set_value('IMPORT', 'max_coins', '10')  # Import only 10 coins
    config.set_value('IMPORT', 'min_market_cap', '1000000000')  # 1B minimum
    config.set_value('FILTERING', 'exclude_stablecoins', 'true')
    
    print("✓ Configuration loaded")
    
    # 2. Create components
    print("\n2. Creating components...")
    try:
        data_provider, exchange_mappers, database_adapter = create_components_from_config(config)
        print(f"✓ Data provider: {type(data_provider).__name__}")
        print(f"✓ Exchange mappers: {[type(m).__name__ for m in exchange_mappers]}")
        print(f"✓ Database adapter: {type(database_adapter).__name__}")
    except Exception as e:
        print(f"✗ Failed to create components: {e}")
        return False
    
    # 3. Initialize orchestrator
    print("\n3. Initializing orchestrator...")
    orchestrator = ImportOrchestrator("examples/example_config.ini")
    
    if not orchestrator.initialize(data_provider, exchange_mappers, database_adapter):
        print("✗ Failed to initialize orchestrator")
        return False
    
    print("✓ Orchestrator initialized")
    
    # 4. Run import
    print("\n4. Running import process...")
    try:
        result = orchestrator.run_import()
        
        print(f"\n=== Import Results ===")
        print(f"Total processed: {result.total_processed}")
        print(f"New records: {result.new_records}")
        print(f"Updated records: {result.updated_records}")
        print(f"Kraken tradeable: {result.kraken_count}")
        print(f"Failed: {result.failed_count}")
        print(f"Execution time: {result.execution_time:.2f} seconds")
        
        if result.errors:
            print(f"\nErrors encountered:")
            for error in result.errors[:3]:  # Show first 3 errors
                print(f"  - {error}")
            if len(result.errors) > 3:
                print(f"  ... and {len(result.errors) - 3} more")
        
        return result.failed_count == 0
        
    except Exception as e:
        print(f"✗ Import failed: {e}")
        return False
    
    finally:
        # 5. Cleanup
        print("\n5. Cleaning up...")
        orchestrator.cleanup()
        print("✓ Cleanup completed")


def update_example():
    """Example of updating existing data"""
    
    print("\n=== Update Example ===\n")
    
    # Quick setup
    config = ConfigurationManager("examples/example_config.ini")
    data_provider, exchange_mappers, database_adapter = create_components_from_config(config)
    orchestrator = ImportOrchestrator("examples/example_config.ini")
    orchestrator.initialize(data_provider, exchange_mappers, database_adapter)
    
    # Run update for specific symbols
    symbols_to_update = ["BTC", "ETH", "ADA"]  # Example symbols
    
    print(f"Updating symbols: {', '.join(symbols_to_update)}")
    
    success = orchestrator.run_update(symbols=symbols_to_update)
    
    if success:
        print("✓ Update completed successfully")
    else:
        print("✗ Update failed")
    
    orchestrator.cleanup()
    return success


def status_example():
    """Example of checking system status"""
    
    print("\n=== Status Example ===\n")
    
    config = ConfigurationManager("examples/example_config.ini")
    
    # Check database status
    db_path = config.get('DATABASE', 'database_path')
    print(f"Database path: {db_path}")
    print(f"Database exists: {'✓' if os.path.exists(db_path) else '✗'}")
    
    # Check configuration
    print(f"\nConfiguration:")
    print(f"  Max coins: {config.get('IMPORT', 'max_coins')}")
    print(f"  Min market cap: ${config.getfloat('IMPORT', 'min_market_cap'):,.0f}")
    print(f"  Exclude stablecoins: {config.getboolean('FILTERING', 'exclude_stablecoins')}")
    
    # Try to create components
    try:
        data_provider, exchange_mappers, database_adapter = create_components_from_config(config)
        print(f"\nComponents:")
        print(f"  ✓ Data provider available")
        print(f"  ✓ {len(exchange_mappers)} exchange mapper(s) available")
        print(f"  ✓ Database adapter available")
        return True
    except Exception as e:
        print(f"\n✗ Component creation failed: {e}")
        return False


if __name__ == "__main__":
    print("Crypto Data Importer - Examples")
    print("=" * 50)
    
    # Run examples
    try:
        # 1. Status check
        if not status_example():
            print("\n❌ Status check failed. Please check your configuration.")
            sys.exit(1)
        
        # 2. Basic import
        print("\n" + "=" * 50)
        if basic_import_example():
            print("\n✅ Basic import completed successfully!")
        else:
            print("\n❌ Basic import failed!")
        
        # 3. Update example
        print("\n" + "=" * 50)
        if update_example():
            print("\n✅ Update example completed successfully!")
        else:
            print("\n❌ Update example failed!")
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
    
    print("\n🎉 All examples completed!")
