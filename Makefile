.PHONY: help install install-dev test test-unit test-integration test-all test-cov lint lint-fix format format-check type-check typecheck check ci dev clean dev-setup pre-commit-install pre-commit-uninstall pre-commit-run

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	uv sync --no-dev

install-dev: ## Install all dependencies including dev tools and pre-commit hooks
	uv sync --group dev
	$(MAKE) pre-commit-install
	@echo "Development environment ready!"
	@echo "Run 'make check' to verify everything works"

test: test-unit ## Run unit tests (default - fast, no API keys required)

test-unit: ## Run unit tests only (fast, offline, mocked)
	uv run pytest tests/unit/ -v

test-integration: ## Run integration tests (slow, requires API keys)
	uv run pytest tests/integration/ -m integration -v

test-all: ## Run all tests (unit + integration)
	uv run pytest tests/ -m "" -v

test-cov: ## Run unit tests with coverage report
	uv run pytest tests/unit/ -v --cov=gpuport_collectors --cov-report=term-missing --cov-report=html

test-cov-all: ## Run all tests with coverage report
	uv run pytest tests/ -m "" -v --cov=gpuport_collectors --cov-report=term-missing --cov-report=html

test-watch: ## Run unit tests in watch mode (requires pytest-watch)
	uv run pytest-watch tests/unit/ -v

lint: ## Run ruff linter
	uv run ruff check src/ tests/

lint-fix: ## Run ruff linter with auto-fixes
	uv run ruff check --fix src/ tests/

format: ## Format code with ruff
	uv run ruff format src/ tests/

format-check: ## Check formatting with ruff
	uv run ruff format --check src/ tests/

typecheck: ## Run mypy type checker
	uv run mypy src/ tests/

check: format-check lint typecheck test ## Run all checks (format, lint, type-check, test)

ci: check ## Run all CI checks (format+lint in check mode, type-check, test)

dev: format lint-fix typecheck test ## Run format+lint in fix mode, then type-check and test

clean: ## Clean up generated files
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .ruff_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf dist
	rm -rf build
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: ## Build the package
	uv build

lock: ## Update dependency lock file
	uv lock

upgrade: ## Upgrade all dependencies to latest versions
	uv lock --upgrade

sync: ## Sync environment with lock file
	uv sync

pre-commit-install: ## Install pre-commit hooks
	uv run pre-commit install
	uv run pre-commit install --hook-type pre-push
	@echo "Pre-commit hooks installed successfully!"
	@echo "Hooks will run automatically on git commit and git push"

pre-commit-uninstall: ## Uninstall pre-commit hooks
	uv run pre-commit uninstall
	uv run pre-commit uninstall --hook-type pre-push

pre-commit-run: ## Run pre-commit hooks manually on all files
	uv run pre-commit run --all-files
