# GPUPort Collectors Constitution

## Core Principles

**Data from all providers MUST be normalized to a consistent schema enabling fair comparison. Inconsistent data structures break cross-provider search and comparison features.**

**Requirements:**
- All GPU types MUST map to standardized names (e.g., "RTX 4090" not "4090" vs "RTX4090" vs "4090 24GB")
- Pricing MUST normalize to USD per hour (focus on on-demand pricing)
- Region/location codes MUST map to standard geo identifiers (US, EU, AP, etc.)
- Availability MUST use consistent enum states (available, limited, unavailable)
- Data transformation layer MUST be provider-agnostic and testable independently

**Rationale:** Users expect apples-to-apples comparison. Inconsistent normalization leads to misleading results, user confusion, and loss of trust.

## Technology Stack & Standards

**Backend Technology:**
- **Python 3.12**: Minimum version for all backend code—leverages modern type hints, performance improvements, and async enhancements
- **Pydantic**: Data validation and settings management—ensures type safety and automatic JSON schema generation
- **httpx**: Async HTTP client for provider API calls—replaces requests for async-compatible external requests
- **pytest**: Test framework for unit and integration tests—industry standard with excellent async support
- **Async**: All I/O operations (database, HTTP, file) MUST use async/await patterns. Blocking operations MUST be wrapped with asyncio.to_thread() or run_in_executor()

**Code Quality & Tooling:**
- **Ruff**: All linting and formatting—replaces Black, isort, flake8, and others with single fast tool
- **mypy**: Static type checking—strict mode required, no untyped code allowed
- **uv**: Package management and virtual environments—faster than pip/poetry, reproducible builds
- Ruff MUST run on pre-commit hooks (auto-format on commit)
- mypy MUST pass with zero errors in CI pipeline—type: ignore allowed only with explicit justification comments
- All dependencies MUST be pinned in pyproject.toml with version constraints

**Testing Standards:**
- **Unit Tests**: MUST mock all external dependencies (database, HTTP, filesystem)—tests run in isolation, no network calls
- **Integration Tests**: MUST use real endpoints and database (test database, not mocked)—validates end-to-end flows
- Unit test coverage: 80% minimum for business logic modules
- Integration test coverage: Critical paths only
- All tests MUST use pytest fixtures for setup/teardown
- Async tests MUST use pytest-asyncio plugin with proper event loop management

**Python Best Practices:**
- Type hints REQUIRED on all function signatures (parameters and return types)
- Docstrings REQUIRED for public APIs and complex logic (Google or NumPy style)
- Context managers (with/async with) REQUIRED for resource management (DB connections, HTTP sessions, files)
- Pydantic models REQUIRED for structured data—no raw dicts for domain objects
- Explicit exception handling REQUIRED—catch specific exceptions, no bare except:
- f-strings REQUIRED for string formatting—no .format() or % formatting
- Pathlib REQUIRED for file paths—no os.path string manipulation
- List/dict comprehensions PREFERRED over map/filter where readable

## Data Quality Standards

**Provider Integration Quality:**
- New provider integrations MUST include unit tests for parsing and normalization
- Scrapers MUST handle common failure modes (timeouts, rate limits, schema changes)
- Data validation MUST reject incomplete records (missing price, GPU type, or availability)
- Failed provider fetches MUST NOT block other providers—isolation required
