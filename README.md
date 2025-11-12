# GPUPort Data Collectors

[![Lint](https://github.com/gpuport/collectors/workflows/Lint/badge.svg)](https://github.com/gpuport/collectors/actions/workflows/lint.yml)
[![Type Check](https://github.com/gpuport/collectors/workflows/Type%20Check/badge.svg)](https://github.com/gpuport/collectors/actions/workflows/type-check.yml)
[![Test](https://github.com/gpuport/collectors/workflows/Test/badge.svg)](https://github.com/gpuport/collectors/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/gpuport/collectors/branch/main/graph/badge.svg)](https://codecov.io/gh/gpuport/collectors)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

Data collectors for the GPUPort platform.

## Development Setup

### Prerequisites

- Python 3.12 or higher
- [uv](https://docs.astral.sh/uv/) - Fast Python package installer and resolver

### Quick Start

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Complete development setup (installs dependencies and pre-commit hooks)
make dev-setup
```

### Manual Installation

```bash
# Install dependencies
uv sync

# Install pre-commit hooks
make pre-commit-install
```

## Development Workflow

### Using Make Commands

The project includes a Makefile with common development tasks:

```bash
# Show all available commands
make help

# Install dependencies
make install-dev

# Run code formatter
make format

# Run linter
make lint

# Run type checker
make type-check

# Run tests
make test

# Run tests with coverage
make test-cov

# Run all checks (lint + type-check + test)
make check

# Run pre-commit hooks manually
make pre-commit-run

# Clean generated files
make clean
```

### Pre-commit Hooks

The project uses pre-commit hooks to ensure code quality. Hooks are automatically installed with `make dev-setup` and run on every commit:

- **Ruff**: Code formatting and linting
- **Mypy**: Static type checking
- **Pytest**: Unit tests (fast mode, no coverage)
- **File checks**: Trailing whitespace, end-of-file, YAML/TOML validation

## License

See [LICENSE](LICENSE) file for details.
