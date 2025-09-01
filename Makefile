.PHONY: help
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

.PHONY: install
install: ## Install all dependencies and set up development environment
	uv sync --all-groups
	uv run --dev pre-commit install

.PHONY: lint
lint: ## Run linting and fix issues
	uv run --dev ruff check --fix src tests examples
	uv run --dev ruff format src tests examples
	uv run --dev mypy src tests

.PHONY: test
test: ## Run all tests
	uv run --dev pytest tests/ -v

.PHONY: redis-up
redis-up: ## Start Redis using Docker Compose
	docker-compose up -d

.PHONY: redis-down
redis-down: ## Stop Redis
	docker-compose down

.PHONY: worker
worker: ## Start a worker (requires Redis running)
	uv run taskflow worker --queues default --concurrency 5

.PHONY: example
example: ## Run example tasks (requires Redis and worker running)
	uv run python -m examples.submit_tasks

.PHONY: dev
dev: install redis-up ## Quick start for development (install + start Redis)
	@echo "✅ Development environment ready!"
	@echo "Run 'make worker' in one terminal and 'make example' in another to test"

# Default target
.DEFAULT_GOAL := help
