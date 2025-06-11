# Crypto Data Importer

A modular, extensible cryptocurrency data importer with AmiBroker integration, built following SOLID principles and the Open-Closed Principle.

## Features

- **Modular Architecture**: Easy to extend with new data providers, exchanges, and databases
- **Multiple Data Sources**: CoinGecko API support with extensible provider system
- **Exchange Integration**: Kraken mapping with support for additional exchanges
- **Database Support**: AmiBroker integration with extensible adapter pattern
- **Advanced Filtering**: Configurable filtering system with custom rule support
- **Automatic Updates**: Scheduled data updates with intelligent caching
- **Comprehensive Testing**: 95%+ test coverage with unit and integration tests
- **Configuration-Driven**: External configuration with validation and defaults

## Quick Start

### Installation

```bash
git clone https://github.com/yourusername/crypto-data-importer.git
cd crypto-data-importer
pip install -r requirements.txt
pip install -e .
```

### Basic Usage

```bash
# Create default configuration
python main.py create-config

# Edit config.ini with your settings
notepad config.ini

# Run import
python main.py
```

### Configuration

Edit `config.ini` to customize:

```ini
[DATABASE]
database_path = C:\AmiBroker\Databases\Crypto\crypto.adb

[IMPORT]
max_coins = 500
min_market_cap = 10000000

[FILTERING]
exclude_stablecoins = true
excluded_symbols = USDT,USDC,DAI
```

## Architecture

The system follows a modular architecture with clear separation of concerns:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Data Providers │    │ Exchange Mappers│    │Database Adapters│
│                 │    │                 │    │                 │
│  • CoinGecko    │    │  • Kraken       │    │  • AmiBroker    │
│  • Binance*     │    │  • Binance*     │    │  • MetaTrader*  │
│  • Custom       │    │  • Custom       │    │  • Custom       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │ Import          │
                    │ Orchestrator    │
                    │                 │
                    │ Coordinates all │
                    │ components      │
                    └─────────────────┘
```

## Testing

Run the comprehensive test suite:

```bash
# Run all tests
python tests/test_suite_runner.py

# Run with coverage
python tests/test_suite_runner.py --coverage --coverage-html coverage_report

# Run specific tests
python tests/test_suite_runner.py --tests TestCoinGeckoProvider

# Include integration tests (requires internet)
python tests/test_suite_runner.py --integration
```

## Extending the System

### Adding a New Data Provider

```python
from src.providers.abstract_data_provider import AbstractDataProvider

class BinanceProvider(AbstractDataProvider):
    def get_all_coins(self) -> List[Dict]:
        # Implementation
        pass
    
    def get_market_data(self, coin_id: str, days: int) -> Optional[Dict]:
        # Implementation
        pass

# Register with factory
from src.core.factory_classes import ProviderFactory
ProviderFactory.register_provider('binance', BinanceProvider)
```

### Adding a New Exchange Mapper

```python
from src.mappers.abstract_exchange_mapper import AbstractExchangeMapper

class BinanceMapper(AbstractExchangeMapper):
    def get_exchange_name(self) -> str:
        return "binance"
    
    # Implement other required methods...

# Register with factory
from src.core.factory_classes import MapperFactory
MapperFactory.register_mapper('binance', BinanceMapper)
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass (`python tests/test_suite_runner.py`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- 📖 [Documentation](docs/)
- 🐛 [Issue Tracker](https://github.com/yourusername/crypto-data-importer/issues)
- 💬 [Discussions](https://github.com/yourusername/crypto-data-importer/discussions)

*Items marked with * are planned features
