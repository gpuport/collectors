.PHONY: help install install-dev test test-cov lint format type-check check clean dev-setup pre-commit-install pre-commit-uninstall pre-commit-run

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	uv sync --no-dev

install-dev: ## Install all dependencies including dev tools
	uv sync
	uv pip install pre-commit

dev-setup: install-dev pre-commit-install ## Complete development environment setup
	@echo "Development environment ready!"
	@echo "Run 'make check' to verify everything works"

test: ## Run tests without coverage
	uv run pytest tests/ -v

test-cov: ## Run tests with coverage report
	uv run pytest tests/ -v --cov=gpuport_collectors --cov-report=term-missing --cov-report=html

test-watch: ## Run tests in watch mode (requires pytest-watch)
	uv run pytest-watch tests/ -v

lint: ## Run ruff linter
	uv run ruff check src/ tests/

format: ## Format code with ruff
	uv run ruff format src/ tests/
	uv run ruff check --fix src/ tests/

type-check: ## Run mypy type checker
	uv run mypy src/ tests/

check: lint type-check test ## Run all checks (lint, type-check, test)

ci: check ## Run all CI checks (alias for check)

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
