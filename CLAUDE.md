# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Main Application Commands
```bash
python main.py                 # Run full import process
python main.py create-config   # Create sample configuration file
python main.py validate-config # Validate existing configuration
python main.py list-components # Show available factory components
python main.py update-only     # Update existing data only
python main.py status          # Show system status
python main.py help           # Show help information
```

### Testing Commands
```bash
python tests/test_suite_runner.py                    # Run all tests
python tests/test_suite_runner.py --coverage         # Run with coverage report
python tests/test_suite_runner.py --integration      # Include integration tests
python tests/test_suite_runner.py --tests TestName   # Run specific test class
python tests/test_suite_runner.py --xml test_reports # Generate XML reports
```

### Development Commands
```bash
pip install -r requirements.txt   # Install dependencies
pip install -e .                  # Install in development mode
flake8 src tests                  # Lint code
mypy src --ignore-missing-imports # Type checking
```

## Architecture Overview

This is a **modular cryptocurrency data importer** built with factory patterns and SOLID principles. The system imports cryptocurrency data from various sources (CoinGecko), maps it to exchange-specific formats (Kraken), and stores it in databases (AmiBroker).

### Core Components (Factory Pattern)

1. **Data Providers** (`src/providers/`): API integrations for fetching crypto data
   - `CoinGeckoProvider`: Primary data source with rate limiting
   - `AbstractDataProvider`: Base interface for extending to new sources

2. **Exchange Mappers** (`src/mappers/`): Map generic coin data to exchange-specific trading pairs
   - `KrakenMapper`: Maps CoinGecko data to Kraken trading pairs
   - `AbstractExchangeMapper`: Base interface for new exchanges

3. **Database Adapters** (`src/adapters/`): Handle database operations
   - `AmiBrokerAdapter`: Windows COM integration with AmiBroker
   - `AbstractDatabaseAdapter`: Base interface for new databases

4. **Import Orchestrator** (`src/orchestrators/`): Coordinates all components
   - `ImportOrchestrator`: Main workflow manager and error handler

5. **Core Services** (`src/core/`):
   - `ConfigurationManager`: INI-based configuration with validation
   - `FactoryClasses`: Factory pattern implementation for component creation
   - `LoggingManager`: Centralized logging configuration

### Component Registration Pattern

New components are registered using factories:
```python
# Register new provider
from core.factory_classes import ProviderFactory
ProviderFactory.register_provider('binance', BinanceProvider)

# Register new mapper
from core.factory_classes import MapperFactory
MapperFactory.register_mapper('binance', BinanceMapper)

# Register new adapter
from core.factory_classes import AdapterFactory
AdapterFactory.register_adapter('postgresql', PostgreSQLAdapter)
```

### Configuration Structure

The system uses `config.ini` with these key sections:
- `[DATABASE]`: Database path and connection settings
- `[IMPORT]`: Import limits (max_coins, min_market_cap)
- `[MAPPING]`: Cache settings and **checkpoint/resume functionality**
- `[PROVIDERS]`: Component selection (data_provider, exchanges, database_adapter)
- `[FILTERING]`: Data filtering rules and exclusions
- `[API]`: Rate limiting and timeout settings
- `[LOGGING]`: Log levels and output configuration

#### Checkpoint/Resume Configuration
```ini
[MAPPING]
checkpoint_enabled = true              # Enable checkpoint/resume functionality
checkpoint_frequency = 100             # Save progress every N coins
resume_on_restart = true               # Auto-resume from checkpoint on restart
checkpoint_file = kraken_mapping_checkpoint.json  # Checkpoint file location
```

### Testing Architecture

- **95%+ test coverage** with comprehensive mocking
- **Custom test runner**: `tests/test_suite_runner.py` with coverage integration
- **Shared fixtures**: `tests/conftest.py` for common test setup
- **Windows-specific**: Tests mock COM objects for cross-platform compatibility
- **Integration tests**: Real API calls (optional with `--integration` flag)
- **Checkpoint tests**: Full coverage of checkpoint/resume functionality in `tests/test_mappers/test_kraken_mapper.py`

### Windows Dependencies

This application requires Windows for AmiBroker COM integration:
- **pywin32**: Windows COM interface library
- **AmiBroker**: Must be installed for database operations
- **CI/CD**: Runs on `windows-latest` in GitHub Actions

### Error Handling Patterns

- **Comprehensive logging**: All components use centralized logging
- **Rate limiting**: Built-in API rate limiting with exponential backoff
- **Graceful degradation**: Continues processing when individual coins fail
- **Result tracking**: `ImportResult` objects track successes/failures
- **Cleanup handling**: Proper resource cleanup in orchestrator

### Extension Points

The system is designed for easy extension:
1. **New data sources**: Implement `AbstractDataProvider`
2. **New exchanges**: Implement `AbstractExchangeMapper`  
3. **New databases**: Implement `AbstractDatabaseAdapter`
4. **Custom filters**: Extend filtering system in `src/filters/`
5. **New schedulers**: Extend update scheduling in `src/schedulers/`

All components follow the same registration pattern and factory-based creation.

## Checkpoint/Resume System

### Overview
The **KrakenMapper** includes a robust checkpoint/resume system that prevents loss of progress during long-running mapping operations. The mapping process can take hours due to API rate limiting, so this system saves incremental progress and resumes from the last checkpoint if the process crashes or is interrupted.

### Key Features

**🔧 Automatic Checkpointing**:
- Saves progress every N coins (configurable, default: 100)
- Tracks processed coins, failed coins, and current mapping data
- Stores timestamps and validation metadata
- Incremental cache updates during processing

**🔄 Smart Resume Logic**:
- Detects incomplete mappings on startup automatically
- Skips already processed coins to avoid duplicate work
- Continues from exact stopping point with sub-coin precision
- Loads partial cache data to preserve existing mappings

**🛡️ Data Integrity & Safety**:
- Validates checkpoint structure and timestamps (24-hour expiry)
- Graceful handling of Ctrl+C interrupts with checkpoint save
- Automatic checkpoint saving on crashes and errors
- Recovery from corrupted checkpoint files (falls back to fresh start)
- Atomic file operations to prevent data corruption

**📊 Enhanced Progress Reporting**:
- Shows "Processing coin X/Y (Z%)" with successful mapping count
- Logs checkpoint saves and resume operations
- Reports failed vs successful mappings for debugging

### Checkpoint Data Structure
```json
{
  "status": "in_progress",
  "total_coins": 5000,
  "processed_coins": 1250,
  "last_processed_index": 1249,
  "processed_coin_ids": ["bitcoin", "ethereum", ...],
  "failed_coin_ids": ["some-failed-coin"],
  "start_time": "2024-01-01T10:00:00",
  "last_checkpoint_time": "2024-01-01T12:30:00",
  "batch_size": 50,
  "checkpoint_frequency": 100,
  "mapping_file": "kraken_mapping.json",
  "partial_mapping_count": 856
}
```

### How It Works

1. **First Run**: Processes coins normally, saving checkpoints every 100 coins
2. **If Interrupted**: Next startup detects checkpoint and resumes from last saved point
3. **Progress Preserved**: Already processed coins are skipped, partial cache is loaded
4. **Completion**: Final cache saved, checkpoint file automatically cleaned up

### Key Methods in KrakenMapper

```python
# Checkpoint management
_save_checkpoint(processed_index, total_coins, processed_coin_ids, mapping_data)
_load_checkpoint() -> Optional[Dict]
_validate_checkpoint(checkpoint_data) -> bool
_should_resume() -> bool
_get_resume_point(all_coins) -> Tuple[int, List[str], Dict]
_clear_checkpoint() -> bool
_update_incremental_cache(mapping_data) -> bool
```

### Recovery Scenarios

**Normal Operation**: Checkpoints saved every 100 coins, process completes normally
**Crash Recovery**: Process resumes from last checkpoint, skipping processed coins
**Corrupted Checkpoint**: Falls back to fresh start with warning message
**Manual Interruption (Ctrl+C)**: Saves checkpoint before exiting gracefully
**Old Checkpoint**: Checkpoints older than 24 hours are considered expired

This system dramatically reduces wasted time when building large mapping caches and provides resilience against network issues, system crashes, and user interruptions.

### Testing the Checkpoint System

The checkpoint functionality has comprehensive test coverage with 25+ test cases covering:

**Unit Tests** (`TestKrakenMapperCheckpoints`):
- Configuration loading and validation
- Checkpoint file creation, loading, and validation
- Resume point calculation and data integrity
- Incremental cache updates
- Error handling for corrupted/expired checkpoints

**Basic Functionality Tests** (`TestKrakenMapperBasic`):
- Exchange name and mapping functionality
- API communication and error handling
- Symbol mapping and tradeability checks

**Integration Tests** (`TestKrakenMapperIntegration`):
- End-to-end checkpoint saving during mapping builds
- Resume functionality with partial cache loading
- Interrupt handling (Ctrl+C) with graceful checkpoint saves
- Multi-instance checkpoint persistence

**Running Checkpoint Tests**:
```bash
# Test all checkpoint functionality
python3 -m unittest tests.test_mappers.test_kraken_mapper.TestKrakenMapperCheckpoints -v

# Test integration scenarios
python3 -m unittest tests.test_mappers.test_kraken_mapper.TestKrakenMapperIntegration -v

# Test all KrakenMapper functionality
python3 -m unittest tests.test_mappers.test_kraken_mapper -v
```