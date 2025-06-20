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
- `[PROVIDERS]`: Component selection (data_provider, exchanges, database_adapter)
- `[FILTERING]`: Data filtering rules and exclusions
- `[API]`: Rate limiting and timeout settings
- `[LOGGING]`: Log levels and output configuration

### Testing Architecture

- **95%+ test coverage** with comprehensive mocking
- **Custom test runner**: `tests/test_suite_runner.py` with coverage integration
- **Shared fixtures**: `tests/conftest.py` for common test setup
- **Windows-specific**: Tests mock COM objects for cross-platform compatibility
- **Integration tests**: Real API calls (optional with `--integration` flag)

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