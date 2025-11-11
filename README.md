# GPUPort Data Collectors

Data collectors for the GPUPort platform.

## Development Setup

### Prerequisites

- Python 3.12 or higher
- [uv](https://docs.astral.sh/uv/) - Fast Python package installer and resolver

### Installation

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create a virtual environment and install dependencies
uv sync

# Or manually:
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e ".[dev]"
```

## Development Tools

### Linting and Formatting with Ruff

```bash
# Check code style
ruff check .

# Format code
ruff format .

# Auto-fix issues
ruff check --fix .
```

### Type Checking with mypy

```bash
# Run type checker
mypy src
```

### Testing with pytest

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov

# Run specific test file
pytest tests/test_example.py

# Run tests in watch mode
pytest -f
```

### Run All Checks

```bash
# Format, lint, type check, and test
ruff format . && ruff check --fix . && mypy src && pytest
```

## License

See [LICENSE](LICENSE) file for details.
