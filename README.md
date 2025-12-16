# Steam Analytics Platform

[![CI](https://github.com/gatotroller/steam-analytics-fabric/actions/workflows/ci.yml/badge.svg)](https://github.com/gatotroller/steam-analytics-fabric/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> Enterprise-grade data pipeline that builds historical analytics for Steam games using Microsoft Fabric and Medallion Architecture.

## The Problem

Steam's APIs only expose **current snapshots** — today's prices, current player counts, latest reviews. There's no public historical data available.

**This project solves that** by capturing daily snapshots and building a complete history using SCD Type 2, enabling analyses like:

- 📈 Price elasticity over time
- 🎮 Game popularity lifecycle patterns
- ⭐ Review sentiment evolution
- 🏷️ Discount strategy effectiveness

## Architecture
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   BRONZE    │    │   SILVER    │    │    GOLD     │
│             │    │             │    │             │
│  Raw JSON   │───►│  Cleaned    │───►│  Business   │
│  Immutable  │    │  SCD Type 2 │    │  Metrics    │
│             │    │  Validated  │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
     │                   │                   │
     └───────────────────┴───────────────────┘
                         │
              Delta Lake on Fabric Lakehouse
```

### Data Sources (Official Steam APIs)

| Source | Data | Frequency |
|--------|------|-----------|
| Store API | Game details, prices, tags, DLC | Daily |
| Reviews API | User reviews, ratings, sentiment | Daily |
| Player Stats API | Current players, achievements | Daily |

## Start

### Prerequisites

- Python 3.10+
- Microsoft Fabric workspace (F2+ capacity)
- Steam Web API Key ([get one here](https://steamcommunity.com/dev/apikey))

### Installation
```bash
# Clone the repository
git clone https://github.com/your-username/steam-analytics-fabric.git
cd steam-analytics-fabric

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate   # Windows

# Install dependencies
pip install -e ".[dev]"

# Setup pre-commit hooks
pre-commit install

# Copy environment template
cp .env.example .env
# Edit .env with your Steam API key
```

### Running Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_extractors.py -v
```

## 📁 Project Structure
```
steam-analytics-fabric/
├── .github/workflows/     # CI/CD pipelines
├── docs/
│   └── architecture/      # ADRs and C4 diagrams
├── src/
│   ├── ingestion/
│   │   ├── extractors/    # API clients
│   │   ├── contracts/     # Pydantic schemas
│   │   └── utils/         # Retry, rate limiting
│   ├── transformation/    # Bronze → Silver → Gold
│   ├── quality/           # Data validation
│   └── orchestration/     # Pipeline definitions
├── tests/
│   ├── unit/
│   ├── integration/
│   └── data_quality/
├── notebooks/             # Fabric notebooks
└── infrastructure/        # Fabric deployment configs
```

## Design Decisions

Key architectural decisions are documented as ADRs:

- [ADR-001: Medallion Architecture](docs/architecture/ADR-001-medallion-architecture.md)
- [ADR-002: SCD Type 2 Strategy](docs/architecture/ADR-002-scd-type2-strategy.md)

## Configuration

| Variable | Description | Required |
|----------|-------------|----------|
| `STEAM_API_KEY` | Steam Web API key | Yes |
| `FABRIC_WORKSPACE_ID` | Fabric workspace GUID | Yes |
| `FABRIC_LAKEHOUSE_ID` | Lakehouse GUID | Yes |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, etc.) | No |

## Quality Assurance

- **Code Quality**: Ruff (linting + formatting), MyPy (type checking)
- **Testing**: pytest with 80% coverage requirement
- **Data Quality**: Great Expectations + Pydantic contracts
- **CI/CD**: GitHub Actions for automated testing and deployment

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Steam Web API Documentation](https://developer.valvesoftware.com/wiki/Steam_Web_API)
- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
- [Delta Lake](https://delta.io/)
